const { EventEmitter } = require('events')
const debug = require('debug')('queuey:queue')
const Promise = require('bluebird')
const co = Promise.coroutine
const collect = Promise.promisify(require('stream-collector'))
const indexer = require('feed-indexer')
const changesFeed = require('changes-feed')
const subdown = require('subleveldown')
const lexint = require('lexicographic-integer')
const LEVEL_OPTS = {
  keyEncoding: 'utf8',
  valueEncoding: 'json'
}

const topics = {
  enqueue: 0,
  dequeue: 1
}

module.exports = createQueue

function createQueue ({ db, worker, autostart }) {
  if (db.options.valueEncoding !== 'json') {
    throw new Error('expected "json" valueEncoding')
  }

  db = Promise.promisifyAll(db)
  const rawFeed = changesFeed(subdown(db, 'c', LEVEL_OPTS))
  const rawDB = subdown(db, 'm', LEVEL_OPTS)
  const feed = Promise.promisifyAll(rawFeed)
  const primaryKey = 'key'
  const entryProp = '_'
  const indexed = indexer({
    db: rawDB,
    feed: rawFeed,
    primaryKey,
    entryProp: entryProp,
    reduce: function (state, change, cb) {
      const { topic } = change.value
      if (topic === topics.enqueue) {
        delete change.value.topic
        return cb(null, change.value)
      }

      // get deleted
      cb()
    }
  })

  let stopped
  let started
  let start
  let awaitStarted = new Promise(resolve => {
    start = function () {
      debug('started')
      resolve()
    }
  })

  worker = wrapWorker(worker)

  function wrapWorker (worker) {
    return co(function* ({ key, value }) {
      // hang
      if (stopped) return

      yield awaitStarted
      debug('processing item ' + key)
      yield worker(value)

      yield feed.appendAsync({
        topic: topics.dequeue,
        key
      })

      emitter.emit('pop', value)
    })
  }

  const enqueue = co(function* enqueue (item) {
    debug('enqueueing')
    const key = yield feed.countAsync()
    yield feed.appendAsync({
      topic: topics.enqueue,
      value: item,
      key: hexint(key)
    })

    debug('enqueued')
  })

  function stop (item) {
    stopped = true
    return db.closeAsync()
  }

  const emitter = new EventEmitter()
  emitter.enqueue = enqueue
  emitter.stop = stop
  emitter.start = start
  emitter.queued = co(function* () {
    indexed.createReadStream().on('data', console.log)
    const vals = yield getValues(indexed)
    return vals.map(val => val.value)
  })

  emitter.length = () => getDBSize(indexed)

  if (autostart) start()

  return emitter
}

function getFirst (db) {
  return collect(db.createReadStream({ limit: 1 }))
}

function getLast (db) {
  return collect(db.createReadStream({ limit: 1, reverse: true }))
}

const getDBSize = co(function* (db) {
  const [first, last] = yield Promise.all([
    getFirst(db),
    getLast(db)
  ])

  // console.log(last, first)
  return last - first
})

function getValues (db) {
  return collect(db.createReadStream({ keys: false }))
}

function promisesub (...args) {
  return Promise.promisifyAll(subdown(...args))
}

function hexint (num) {
  return lexint.pack(num, 'hex')
}

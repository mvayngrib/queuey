const { EventEmitter } = require('events')
const debug = require('debug')('queuey:queue')
const Promise = require('bluebird')
const co = Promise.coroutine
const collect = Promise.promisify(require('stream-collector'))
const changesFeed = require('changes-feed')
const subdown = require('subleveldown')
const lexint = require('lexicographic-integer')
const processChanges = require('level-change-processor')
const LEVEL_OPTS = {
  keyEncoding: 'utf8',
  valueEncoding: 'json'
}

module.exports = createQueue

function createQueue ({ db, worker, autostart }) {
  if (db.options.valueEncoding !== 'json') {
    throw new Error('expected "json" valueEncoding')
  }

  let stopped
  let started
  let start
  let awaitStarted = new Promise(resolve => {
    start = function () {
      debug('started')
      resolve()
    }
  })

  db = Promise.promisifyAll(db)
  const rawFeedDB = subdown(db, 'c', LEVEL_OPTS)
  const feedDB = Promise.promisifyAll(rawFeedDB)
  const rawFeed = changesFeed(rawFeedDB)
  const counterDB = subdown(db, 'm', LEVEL_OPTS)
  const feed = Promise.promisifyAll(rawFeed)

  const processor = processChanges({
    db: counterDB,
    feed: rawFeed,
    worker: co(function* ({ change, value }, cb) {
      // hang
      if (stopped) return

      yield awaitStarted
      debug('processing item ' + change)
      try {
        yield worker(value)
      } catch (err) {
        return cb(err)
      }

      debug('processed item ' + change)
      yield feedDB.delAsync(hexint(change))
      cb()

      emitter.emit('pop', value)
    })
  })

  function stop (item) {
    stopped = true
    return db.closeAsync()
  }

  const emitter = new EventEmitter()
  emitter.enqueue = function (item) {
    return feed.appendAsync(item)
  }

  emitter.stop = stop
  emitter.start = start
  emitter.queued = () => getValues(rawFeed)
  emitter.length = () => getDBSize(rawFeed)

  if (autostart) start()

  return emitter
}

function getFirst (feed) {
  return collect(feed.createReadStream({ limit: 1 }))
}

function getLast (feed) {
  return collect(feed.createReadStream({ limit: 1, reverse: true }))
}

const getDBSize = co(function* (feed) {
  const [first, last] = yield Promise.all([
    getFirst(feed),
    getLast(feed)
  ])

  return last - first
})

function getValues (feed) {
  return collect(feed.createReadStream({ keys: false }))
}

function promisesub (...args) {
  return Promise.promisifyAll(subdown(...args))
}

function hexint (num) {
  return lexint.pack(num, 'hex')
}

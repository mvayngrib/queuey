const path = require('path')
const { EventEmitter } = require('events')
const debug = require('debug')('queuey:queue')
const extend = require('xtend')
const Promise = require('bluebird')
const co = Promise.coroutine
const collect = Promise.promisify(require('stream-collector'))
const changesFeed = require('changes-feed')
const lexint = require('lexicographic-integer')
const processChanges = require('level-change-processor')
const levelup = require('levelup')
const LEVEL_OPTS = {
  keyEncoding: 'utf8',
  valueEncoding: 'json'
}

module.exports = createQueue

function createQueue ({ dir, leveldown, worker, autostart }) {
  let stopped
  let started
  let start
  let awaitStarted = new Promise(resolve => {
    start = function () {
      debug('started')
      resolve()
    }
  })

  const feedDB = newDB('feed')
  const feed = Promise.promisifyAll(changesFeed(feedDB))
  const counterDB = newDB('feedState')

  const processor = processChanges({
    db: counterDB,
    feed,
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

      // this unfortunately relies on the
      // internal structure of changes-feed
      yield feedDB.delAsync(hexint(change))
      cb()

      emitter.emit('pop', value)
    })
  })

  function stop (item) {
    stopped = true
    return Promise.all([
      feedDB.closeAsync(),
      counterDB.closeAsync()
    ])
  }

  const emitter = new EventEmitter()
  emitter.enqueue = function (item) {
    return feed.appendAsync(item)
  }

  emitter.stop = stop
  emitter.start = start
  emitter.queued = () => getValues(feed)
  emitter.length = () => getDBSize(feed)

  if (autostart) start()

  return emitter

  function newDB (dbPath) {
    const opts = extend(LEVEL_OPTS)
    if (leveldown) opts.leveldown = leveldown
    const db = levelup(path.join(dir, dbPath), opts)
    return Promise.promisifyAll(db)
  }
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

function hexint (num) {
  return lexint.pack(num, 'hex')
}

const { EventEmitter } = require('events')
const extend = require('xtend/mutable')
const debug = require('debug')('queuey')
const Promise = require('bluebird')
const co = Promise.coroutine
const collect = Promise.promisify(require('stream-collector'))
const subdown = require('subleveldown')
const pump = require('pump')
const through = require('through2')
const indexer = require('feed-indexer')
const lexint = require('lexicographic-integer')
const changesFeed = require('changes-feed')
const LEVEL_OPTS = {
  keyEncoding: 'utf8',
  valueEncoding: 'json'
}

const ENTRY_PROP = '_'

module.exports = exports = createQueueManager

function createQueueManager ({ db }) {
  const emitter = new EventEmitter()
  const feedDB = Promise.promisifyAll(subdown(db, 'f', LEVEL_OPTS))
  const feed = Promise.promisifyAll(changesFeed(feedDB))
  const queuesDB = subdown(db, 'q', LEVEL_OPTS)
  const workerStreams = {}
  const workers = {}
  const queues = {}

  let stopped
  db.once('closing', function () {
    debug('db is closing')
    stop()
  })

  Promise.promisifyAll(db)

  let pendingWrites = []
  const pushPendingWrite = co(function* pushPendingWrite (promise) {
    pendingWrites.push(promise)
    yield promise
    pendingWrites.splice(pendingWrites.indexOf(promise), 1)
  })

  const finishWrites = co(function* finishWrites () {
    yield Promise.all(pendingWrites)
    yield new Promise(resolve => setTimeout(resolve, 50))
  })

  let waitForWorker = {}
  const STATUS = {
    todo: '0',
    done: '1'
  }

  const primaryKey = ENTRY_PROP
  const indexed = indexer({
    feed,
    db: queuesDB,
    primaryKey: 'change',
    entryProp: ENTRY_PROP,
    reduce: co(function* (state, entry, cb) {
      if (stopped) return

      // delete
      if (state) {
        cb()
      } else {
        cb(null, entry.value)
      }

      yield feedDB.delAsync(hexint(entry.change))
    })
  })

  const indexes = {}
  const sep = indexed.separator
  indexes.status = indexed.by('status', function (state) {
    return state.status + sep + state.name + sep + state.change
  })

  // indexes.name = indexed.by('name', function (state) {
  //   return state.name + sep + getEntryLink(state)
  // })

  function todoStream (opts={}) {
    const { name, live=true, keys=false } = opts

    let eq = STATUS.todo
    if (name) eq += sep + name

    return indexes.status.createReadStream({
      eq,
      live,
      keys
    })
  }

  function createWorkerStream (name) {
    return through.obj({ highWaterMark: 1 }, co(function* (item, enc, cb) {
      if (stopped) return

      if (!workers[name]) {
        debug(`waiting for worker to be set for queue "${name}"`)
        yield new Promise(resolve => waitForWorker[name] = resolve)
      }

      const { change, data } = item
      try {
        debug(`processing task in queue "${name}"`)
        yield workers[name](data)
        debug(`processed task in queue "${name}"`)
        // tragic, but continuing will cause a segfault
        if (stopped) return

        debug('marking task as finished')
        const append = feed.appendAsync({ change, status: STATUS.done })
        pushPendingWrite(append)
        yield append

        debug('marked task as finished')
      } catch (err) {
        if (err instanceof TypeError || err instanceof ReferenceError) {
          debug('developer error:', err.stack)
          throw err
        }

        emitter.emit('error', err)
        debug(`pausing queue "${name}" due to error`, err)
        return
      }

      cb()
      emitter.emit('pop', name, data)
      queues[name].emit('pop', data)
    }))
  }

  pump(
    todoStream(),
    through.obj(co(function* (data, enc, cb) {
      if (stopped) return

      const { name } = data
      if (!workerStreams[name]) {
        workerStreams[name] = createWorkerStream(name)
      }

      workerStreams[name].write(data)
      cb()
    }))
  )

  const queued = co(function* queued (name) {
    if (name) {
      const results = yield collect(todoStream({ name, live: false, keys: false }))
      return results.map(result => result.data)
    }

    const results = yield collect(todoStream({ live: false, keys: false }))
    const byName = {}
    results.forEach(({ name, data }) => {
      if (!byName[name]) byName[name] = []

      byName[name].push(data)
    })

    return byName
  })

  let position
  const getInitialPos = co(function* () {
    position = yield feed.countAsync()
  })()

  const enqueue = co(function* enqueue (name, data) {
    if (!workers[name]) throw new Error('expected worker')
    if (stopped) throw new Error('i have already been stopped')

    if (typeof position === 'undefined') {
      yield getInitialPos
      if (stopped) return
    }

    debug(`pushing into queue "${name}"`)
    const promise = feed.appendAsync({
      name,
      data,
      status: STATUS.todo,
      change: hexint(++position)
    })

    pushPendingWrite(promise)
    return promise
  })

  const stop = co(function* stop () {
    if (stopped) return

    debug('stopping')
    stopped = true
    yield finishWrites()
    debug('stopped')
  })

  const clear = co(function* clear (name) {
    const queued = yield collect(todoStream({ name, live: false }))
    const batch = queued.map(item => {
      return {
        type: 'del',
        key: item.change
      }
    })

    return feedDB.batchAsync(batch)
  })

  function getQueue ({ name, worker }) {
    if (queues[name]) return queues[name]
    if (!worker) throw new Error('expected "worker" and "name"')

    workers[name] = worker
    if (waitForWorker[name]) {
      waitForWorker[name]()
    }

    queues[name] = extend(new EventEmitter(), {
      queued: queued.bind(null, name),
      enqueue: enqueue.bind(null, name),
      clear: clear.bind(null, name)
    })

    return queues[name]
  }

  return extend(emitter, {
    queue: getQueue,
    enqueue,
    stop,
    queued
  })
}

function hexint (num) {
  return lexint.pack(num, 'hex')
}

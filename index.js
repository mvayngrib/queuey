
const { EventEmitter } = require('events')
const changesFeed = require('changes-feed')
const subdown = require('subleveldown')
const levelup = require('levelup')
const Promise = require('bluebird')
const levelJobs = require('level-jobs')
const co = Promise.coroutine
const collect = Promise.promisify(require('stream-collector'))
const noop = function () {}
const LEVEL_JOBS_OPTS = {
  maxConcurrency: 1,
  maxRetries: 0
}

module.exports = function createQueues (db) {
  db = Promise.promisifyAll(db)
  const running = {}
  const mainDB = promisesub(db, 'm')
  const namesDB = promisesub(db, 'n')
  const getQueue = function getQueue ({ name, worker, autostart=true, concurrency=1 }) {
    if (running[name]) return running[name]
    if (!worker) throw new Error('expected "worker"')

    namesDB.putAsync(name, true)
    const queue = createQueue({
      autostart,
      worker,
      db: subdown(mainDB, name)
    })

    queue.on('pop', function (item) {
      emitter.emit('pop', name, item)
    })

    queue.on('stop', function () {
      emitter.emit('stop', name)
    })

    return queue
  }

  const queued = co(function* queued (name) {
    if (name) {
      const queue = yield getQueue({ name })
      return queue.queued()
    }

    const names = yield collect(namesDB.createKeyStream())
    const allQueued = yield Promise.all(names.map(name => {
      return tempOp({
        name,
        op: tmp => tmp.queued()
      })
    }))

    const byName = {}
    allQueued.forEach((queued, i) => {
      byName[names[i]] = queued
    })

    return byName
  })

  const deleteQueueDB = co(function* deleteQueueDB (name) {
    const qdb = promisesub(db, name)
    const keys = yield collect(qdb.createKeyStream())
    const batch = keys.map(key => {
      return { type: 'del', key }
    })

    yield qdb.batchAsync(batch)
  })

  const clear = co(function* clear (name) {
    if (name) {
      if (running[name]) {
        yield running[name].stop()
      }

      yield deleteQueueDB(name)
      return
    }

    return clearAll()
  })

  function clearAll () {
    return db.destroyAsync()
  }

  const stop = co(function* stop (name) {
    if (running[name]) {
      yield running[name].stop()
      delete running[name]
    }
  })

  const emitter = new EventEmitter()
  emitter.queue = getQueue
  emitter.clear = clear
  emitter.stop = stop
  emitter.queued = queued
  const tempOp = co(function* tempOp ({ name, op }) {
    const tmp = createQueue({
      autostart: true,
      db: subdown(mainDB, name),
      worker: noop
    })

    const result = yield op(tmp)
    yield tmp.stop()
    return result
  })

  return emitter
}


function createQueue ({ db, worker, opts=LEVEL_JOBS_OPTS, autostart }) {
  let queue
  let stopped
  let started
  let start
  let awaitStarted = new Promise(resolve => {
    start = function () {
      if (worker !== noop) debugger
      resolve()
    }
  })

  db = Promise.promisifyAll(db)
  worker = wrapWorker(worker)
  queue = Promise.promisifyAll(levelJobs(db, worker, opts))

  function wrapWorker (worker) {
    worker = Promise.promisify(worker)
    return co(function* (item) {
      // hang
      if (stopped) return

      yield awaitStarted
      yield worker(item)
      emitter.emit('pop', item)
    })
  }

  function enqueue (item) {
    return queue.pushAsync(item)
  }

  function stop (item) {
    stopped = true
    return db.closeAsync()
  }

  const emitter = new EventEmitter()
  emitter.enqueue = enqueue
  emitter.stop = stop
  emitter.start = start
  emitter.queued = function queued () {
    return collect(queue.readStream({ keys: false }))
  }

  emitter.length = function () {
    return new Promise((resolve, reject) => {
      let count = 0
      queue.readStream({ values: false })
        .on('data', data => count++)
        .once('error', reject)
        .once('end', () => resolve(count))
    })
  }

  if (autostart) start()

  return emitter
}

function promisesub (...args) {
  return Promise.promisifyAll(subdown(...args))
}


const { EventEmitter } = require('events')
const low = require('lowdb')
const fileAsync = require('lowdb/lib/storages/file-async')
const Promise = require('bluebird')
const co = Promise.coroutine

module.exports = function createQueueManager (path) {
  const db = low(path, {
    storage: fileAsync
  })

  db.defaults({
      queues: {}
    })
    .write()

  const running = {}

  function getQueue ({ name, worker, autostart=true }) {
    if (running[name]) return running[name]
    if (!worker) throw new Error('expected "worker"')

    const qpath = `queues.${name}`
    let qdb = db.get(qpath)
    if (!qdb.value()) db.set(qpath, []).write()

    const queue = running[name] = createQueue({
      db: qdb,
      worker,
      autostart: false,
      save: update
    })

    queue.on('pop', function (item) {
      emitter.emit('pop', name, item)
    })

    queue.on('stop', function () {
      emitter.emit('stop', name)
    })

    return queue

    function update (items) {
      return db.set(path, items).write()
    }
  }

  const clear = co(function* clear (name) {
    if (name) {
      if (running[name]) {
        yield running[name].clear()
      } else {
        yield db.set(`queues.${name}`, []).value()
      }

      return
    }

    return clearAll()
  })

  const clearAll = co(function* () {
    // TODO: optimize
    yield Promise.all(Object.keys(running).map(clear))
    // return db.set('queues', {}).value()
  })

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
  emitter.queued = function (name) {
    if (name) {
      return getQueue({ name }).queued()
    }

    return Promise.resolve(db.get('queues').value())
  }

  return emitter
}

function createQueue ({ db, worker, save, autostart }) {
  let stopped
  let items = db.value() || []
  // if (!items) {
  //   items = []
  //   db.set(items).write()
  // }

  let pending = Promise.resolve()
  let processing

  const processNext = co(function* () {
    if (stopped || processing || !items.length) return

    processing = true
    const next = items[0]
    const maybePromise = worker(next)
    if (isPromise(maybePromise)) yield maybePromise

    items.shift()
    yield save(items)
    processing = false

    emitter.emit('pop', next)
    processNext()
  })

  function isPromise (obj) {
    return obj && typeof obj.then === 'function'
  }

  function stop () {
    if (stopped) return Promise.resolve()

    stopped = true
    return new Promise(resolve => {
      if (!processing) return resolve()

      emitter.once('pop', function () {
        resolve()
        emitter.emit('stop')
      })
    })
  }

  function start () {
    stopped = false
    processNext()
  }

  const enqueue = co(function* enqueue (item) {
    items.push(item)
    yield save(items)
    processNext()
  })

  const clear = co(function* clear () {
    let wasStopped = stopped
    yield stop()
    items.length = 0
    yield save(items)
    if (!wasStopped) start()
  })

  const emitter = new EventEmitter()
  emitter.enqueue = enqueue
  emitter.stop = stop
  emitter.start = start
  emitter.clear = clear
  emitter.queued = function () {
    return Promise.resolve(items.slice())
  }

  emitter.length = () => Promise.resolve(items.length)

  if (autostart) start()

  return emitter
}

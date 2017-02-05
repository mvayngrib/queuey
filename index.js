
const { EventEmitter } = require('events')
const low = require('lowdb')
const Promise = require('bluebird')
const co = Promise.coroutine

module.exports = function createQueueManager (path) {
  const db = low(path)
  db.defaults({
      queues: {}
    })
    .value()

  const running = {}

  function getQueue ({ name, worker, autostart=true }) {
    if (running[name]) return running[name]
    if (!worker) throw new Error('expected "worker"')

    const path = `queues.${name}`
    let items = db.get(path).value()
    if (!items) {
      items = []
      update(items)
    }

    const queue = running[name] = createQueue({
      items,
      worker,
      autostart,
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
      db.set(path, items).value()
    }
  }

  const clear = co(function* clear (name) {
    if (name) {
      if (running[name]) {
        yield running[name].clear()
      } else {
        db.set(`queues.${name}`, []).value()
      }

      return
    }

    yield clearAll()
    db.set('queues', {}).value()
  })

  function clearAll () {
    return Promise.all(Object.keys(running).map(clear))
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
  emitter.queued = function (name) {
    if (name) {
      return getQueue({ name }).queued()
    }

    return db.get('queues').value()
  }

  return emitter
}


function createQueue ({ items, worker, save, autostart }) {
  let stopped
  let pending = Promise.resolve()
  let processing

  const processNext = co(function* () {
    if (stopped || processing || !items.length) return

    processing = true
    const next = items[0]
    const maybePromise = worker(next)
    if (isPromise(maybePromise)) yield maybePromise

    items.shift()
    save(items)
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

  function enqueue (item) {
    items.push(item)
    save(items)
    processNext()
  }

  const clear = co(function* clear () {
    let wasStopped = stopped
    yield stop()
    items.length = 0
    save(items)
    if (!wasStopped) start()
  })

  const emitter = new EventEmitter()
  emitter.enqueue = enqueue
  emitter.stop = stop
  emitter.start = start
  emitter.clear = clear
  emitter.queued = function () {
    return items.slice()
  }

  Object.defineProperty(emitter, 'length', {
    get: function () {
      return items.length
    }
  })

  if (autostart) start()

  return emitter
}

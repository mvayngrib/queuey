
const fs = require('fs')
const test = require('tape')
const Promise = require('bluebird')
const co = Promise.coroutine
const { cleanup, testPath, createDB } = require('./utils')
const makeQueues = require('../')

cleanup()

test('basic', co(function* (t) {
  let db = createDB('a')
  ;['open', 'opening', 'ready', 'closing', 'closed', 'put', 'get', 'batch', 'del'].forEach(event => {
    db.on(event, function () {
      console.log(event)
    })
  })

  const queues = getQueues(db)
  t.same(yield queues.queued(), {})

  const queue = getTimeoutQueue(queues)
  t.same(yield queue.queued(), [])

  const todo = [
    { timeout: 100, value: 0 },
    { timeout: 50, value: 1 },
    { timeout: 10, value: 2 }
  ];

  yield Promise.all(todo.map(item => queue.enqueue(item)))

  // yield new Promise(resolve => setTimeout(resolve, 1000))
  t.same(yield queues.queued(), { wait: todo })
  t.same(yield queue.queued(), todo)

  let i = 0
  queue.once('pop', co(function* () {
    yield queues.stop()
    reopen()
  }))

  queue.on('pop', function (item) {
    t.equal(item.value, i++)
  })

  const reopen = co(function* reopen () {
    // yield new Promise(resolve => setTimeout(resolve, 1))
    yield db.closeAsync()
    // yield new Promise(resolve => setTimeout(resolve, 1000))
    db = createDB('a')
    const queues = getQueues(db)
    const resurrected = getTimeoutQueue(queues)
    t.same(yield queues.queued(), { wait: todo.slice(1) })
    t.same(yield queues.queued('wait'), todo.slice(1))
    t.same(yield resurrected.queued(), todo.slice(1))
    resurrected.on('pop', co(function* (item) {
      t.equal(item.value, i++)
      if (i === todo.length) {
        yield queues.stop()
        db.close(t.end)
      }
    }))
  })

  function getQueues (db) {
    return makeQueues({ db })
  }

  function getTimeoutQueue (queues, autostart=false) {
    return queues.queue({
      worker: timeoutSuccess,
      name: 'wait'
    })
  }
}))

test('clear one', co(function* (t) {
  const todo = [
    { timeout: 100 },
    { timeout: 50 },
    { timeout: 10 }
  ];

  let expectedFinished = 1
  let finished = 0

  const queues = makeQueues({
    db: createDB('b')
  })

  const a = queues.queue({ name: 'a', worker: timeoutCounter })
  yield Promise.all(todo.map(item => a.enqueue(item)))
  t.same(yield a.queued(), todo)
  yield a.clear()
  t.same(yield a.queued(), [])
  setTimeout(function () {
    t.equal(finished, expectedFinished)
    t.end()
  }, 300)

  function timeoutCounter (item) {
    return new Promise(resolve => {
      setTimeout(function () {
        finished++
        resolve()
      }, item.timeout)
    })
  }
}))

test('clear all', co(function* (t) {
  const queues = makeQueues(createDB('c'))
  const todo = [
    { timeout: 100 },
    { timeout: 50 },
    { timeout: 10 }
  ];

  const a = queues.queue({ name: 'a', worker: timeoutCounter })
  const b = queues.queue({ name: 'b', worker: timeoutCounter })

  // run 2 queues
  yield Promise.all(todo.map(item => a.enqueue(item)))
  yield Promise.all(todo.map(item => b.enqueue(item)))

  let expectedFinished = 2
  let finished = 0

  // clear all
  yield queues.clear()
  t.same(a.queued(), [])
  t.same(b.queued(), [])
  setTimeout(function () {
    t.equal(finished, expectedFinished)
    t.end()
  }, 300)

  function timeoutCounter (item) {
    return new Promise(resolve => {
      setTimeout(function () {
        finished++
        resolve()
      }, item.timeout)
    })
  }
}))

test('cleanup', function (t) {
  t.end()
})

function timeoutSuccess (item) {
  return new Promise(resolve => {
    setTimeout(function () {
      resolve()
    }, item.timeout)
  })
}

function timeoutError (item) {
  return new Promise((resolve, reject) => {
    setTimeout(function () {
      reject(new Error('timed out'))
    }, item.timeout)
  })
}

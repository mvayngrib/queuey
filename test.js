
const fs = require('fs')
const test = require('tape')
const Promise = require('bluebird')
const co = Promise.coroutine
const memdb = require('memdb')
const makeQueues = require('./')

test.only('basic', co(function* (t) {
  const queues = getQueues()
  t.same(yield queues.queued(), {})

  const queue = getTimeoutQueue(queues)
  t.same(yield queues.queued(), { wait: [] })

  const todo = [
    { timeout: 50, value: 1 },
    { timeout: 10, value: 2 }
  ];

  yield Promise.all(todo.map(item => queue.enqueue(item)))

  t.same(yield queues.queued(), { wait: todo })
  t.same(yield queue.queued(), todo)

  queue.start()

  let i = 0
  queue.once('pop', co(function* () {
    yield queue.stop()
    reopen()
  }))

  queue.on('pop', function (item) {
    t.equal(item.value, i++)
  })

  const reopen = co(function* reopen () {
    const queues = getQueues()
    const resurrected = getTimeoutQueue(queues)
    t.same(yield queues.queued(), { wait: todo.slice(1) })
    t.same(yield queues.queued('wait'), todo.slice(1))
    t.same(yield resurrected.queued(), todo.slice(1))
    resurrected.on('pop', function (item) {
      t.equal(item.value, i++)
      if (i === todo.length) {
        t.end()
      }
    })
  })

  function getQueues () {
    return makeQueues(memdb())
  }

  function getTimeoutQueue (queues, autostart=false) {
    return queues.queue({
      autostart,
      name: 'wait',
      worker: timeoutSuccess
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

  const queues = makeQueues(memdb())
  const a = queues.queue({ name: 'a', worker: timeoutCounter })
  todo.forEach(item => a.enqueue(item))
  t.same(a.queued(), todo)
  yield a.clear()
  t.same(a.queued(), [])
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
  const queues = makeQueues(memdb())
  const todo = [
    { timeout: 100 },
    { timeout: 50 },
    { timeout: 10 }
  ];

  const a = queues.queue({ name: 'a', worker: timeoutCounter })
  const b = queues.queue({ name: 'b', worker: timeoutCounter })

  // run 2 queues
  todo.forEach(item => a.enqueue(item))
  todo.forEach(item => b.enqueue(item))

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

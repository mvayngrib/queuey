
const fs = require('fs')
const test = require('tape')
const Promise = require('bluebird')
const co = Promise.coroutine
const makeQueues = require('./')
const TEST_DB = './testdb.json'

test('basic', function (t) {
  cleanup()

  const queues = getQueues()
  t.same(queues.queued(), {})

  const queue = getTimeoutQueue(queues)
  t.same(queues.queued(), { wait: [] })

  const todo = [
    { timeout: 100, value: 0 },
    { timeout: 50, value: 1 },
    { timeout: 10, value: 2 }
  ];

  todo.forEach(item => queue.enqueue(item))

  t.same(queues.queued(), { 'wait': todo })
  t.same(queue.queued(), todo)

  let i = 0
  queue.once('pop', function () {
    queue.stop()
    reopen()
  })

  queue.on('pop', function (item) {
    t.equal(item.value, i++)
  })

  function reopen () {
    const queues = getQueues()
    const resurrected = getTimeoutQueue(queues)
    t.same(queues.queued(), { wait: todo.slice(1) })
    t.same(queues.queued('wait'), todo.slice(1))
    t.same(resurrected.queued(), todo.slice(1))
    resurrected.on('pop', function (item) {
      t.equal(item.value, i++)
      if (i === 3) {
        t.end()
      }
    })
  }

  function getQueues () {
    return makeQueues(TEST_DB)
  }

  function getTimeoutQueue (queues) {
    return queues.queue({
      name: 'wait',
      worker: timeoutSuccess
    })
  }
})

test('clear one', co(function* (t) {
  cleanup()

  const todo = [
    { timeout: 100 },
    { timeout: 50 },
    { timeout: 10 }
  ];

  let expectedFinished = 1
  let finished = 0

  const queues = makeQueues(TEST_DB)
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
  cleanup()
  const queues = makeQueues(TEST_DB)
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
  cleanup()
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

function cleanup () {
  if (fs.existsSync(TEST_DB)) {
    fs.unlinkSync(TEST_DB)
  }
}



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

  function cleanup () {
    if (fs.existsSync(TEST_DB)) {
      fs.unlinkSync(TEST_DB)
    }
  }

  function reopen () {
    const queues = getQueues()
    const resurrected = getTimeoutQueue(queues)
    t.same(queues.queued(), { wait: todo.slice(1) })
    t.same(queues.queued('wait'), todo.slice(1))
    t.same(resurrected.queued(), todo.slice(1))
    resurrected.on('pop', function (item) {
      t.equal(item.value, i++)
      if (i === 3) {
        cleanup()
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
      worker: function (item) {
        return new Promise(resolve => {
          setTimeout(function () {
            resolve()
          }, item.timeout)
        })
      }
    })
  }
})

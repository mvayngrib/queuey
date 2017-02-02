
const fs = require('fs')
const test = require('tape')
const Promise = require('bluebird')
const co = Promise.coroutine
const makeQueues = require('./')
const TEST_DB = './testdb.json'

test('basic', function (t) {
  cleanup()

  const waits = getTimeoutQueue('wait')

  waits.enqueue({ timeout: 100, value: 0 })
  waits.enqueue({ timeout: 50, value: 1 })
  waits.enqueue({ timeout: 10, value: 2 })

  let i = 0
  waits.once('pop', function () {
    waits.stop()
    reopen()
  })

  waits.on('pop', function (item) {
    t.equal(item.value, i++)
  })

  function cleanup () {
    if (fs.existsSync(TEST_DB)) {
      fs.unlinkSync(TEST_DB)
    }
  }

  function reopen () {
    const resurrected = getTimeoutQueue('wait')
    resurrected.on('pop', function (item) {
      t.equal(item.value, i++)
      if (i === 3) {
        cleanup()
        t.end()
      }
    })
  }

  function getTimeoutQueue (name) {
    const queues = makeQueues(TEST_DB)
    return queues.queue({
      name: name,
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

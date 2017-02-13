
const path = require('path')
const mkdirp = require('mkdirp')
const rimraf = require('rimraf')
const levelup = require('levelup')
const test = require('tape')
const Promise = require('bluebird')
const co = Promise.coroutine
const createQueue = require('../queue')
const TEST_DB_PATH = path.resolve(__dirname, 'testdbs')

let dbCounter = 0
cleanup()

function createDB (num) {
  const name = String(num || dbCounter++) + '.db'
  const dbPath = path.resolve(TEST_DB_PATH, name)
  mkdirp.sync(dbPath)
  return levelup(dbPath, { valueEncoding: 'json' })
}

test('basic', co(function* (t) {
  const queue = createQueue({
    db: createDB(0),
    worker: timeoutSuccess
  })

  const todo = [
    { timeout: 100, value: 0 },
    { timeout: 50, value: 1 },
    { timeout: 10, value: 2 }
  ];

  yield Promise.all(todo.map(item => queue.enqueue(item)))

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
    const queue = createQueue({
      db: createDB(0),
      worker: timeoutSuccess
    })

    t.same(yield queue.queued(), todo.slice(1))
    queue.on('pop', co(function* (item) {
      t.equal(item.value, i++)
      if (i === todo.length) {
        yield queue.stop()
        t.end()
      }
    }))
  })
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
  rimraf.sync(TEST_DB_PATH)
}
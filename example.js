
const fs = require('fs')
const DB_PATH = './testdb.json'
if (fs.existsSync(DB_PATH)) {
  fs.unlinkSync(DB_PATH)
}

const queues = require('./')(DB_PATH)

const callGrandmas = queues.queue({
  name: 'grandmas',
  worker: function (grandma) {
    // do stuff
    // optionally return Promise
    return promiseSometimeLater(function () {
      console.log('calling grandma ' + grandma)
    })
  }
})

const callGrandpas = queues.queue({
  name: 'grandpas',
  worker: function (grandpa) {
    // do stuff
    // optionally return Promise
    return promiseSometimeLater(function () {
      console.log('calling grandpa ' + grandpa)
    })
  }
})

callGrandpas.enqueue('Bill')
callGrandpas.enqueue('Ted')
callGrandpas.enqueue('Rufus')

callGrandmas.enqueue('Beyonce')
callGrandmas.enqueue('Rachel McAdams')
callGrandmas.enqueue('Gluugfrinksnoorg')

function promiseSometimeLater (fn) {
  return new Promise(resolve => {
    setTimeout(function () {
      fn()
      resolve()
    }, Math.random() * 1000 | 0)
  })
}

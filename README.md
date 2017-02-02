
# queuey

## Usage

```js
const queues = require('queuey')('./testdb.json')

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

// calling grandpa Bill
// calling grandma Beyonce
// calling grandpa Ted
// calling grandma Rachel McAdams
// calling grandma Gluugfrinksnoorg
// calling grandpa Rufus
```

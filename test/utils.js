const path = require('path')
const mkdirp = require('mkdirp')
const levelup = require('levelup')
const rimraf = require('rimraf')
const TEST_DB_PATH = path.resolve(__dirname, 'testdbs')

let dbCounter = 0

exports.testPath = function testPath (name) {
  if (!name) name = dbCounter++ + '.db'
  const dbPath = path.resolve(TEST_DB_PATH, name)
  mkdirp.sync(dbPath)
  return dbPath
}

exports.createDB = function createDB (name) {
  return levelup(exports.testPath(name), { valueEncoding: 'json' })
}

exports.cleanup = function cleanup () {
  rimraf.sync(TEST_DB_PATH)
}


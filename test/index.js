try {
  const SegfaultHandler = require('segfault-handler')
  SegfaultHandler.registerHandler("crash.log")
} catch (err) {}

require('./queue')
require('./queues')

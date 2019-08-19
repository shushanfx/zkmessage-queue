const EventEmitter = require('events').EventEmitter;
const MessageQueueError = require('./message-queue-error');

class AbstractMessageQueue extends EventEmitter {
  constructor() {
    super()
  }
  connect() {
    throw new MessageQueueError("Not implement");
  }
  close() {
    throw new MessageQueueError("Not implement");
  }
  push(content, done) {
    throw new MessageQueueError("Not implement");
  }
  async pushPromise(content) {
    throw new MessageQueueError("Not implement");
  }
}

module.exports = AbstractMessageQueue;
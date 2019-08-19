class MessageQueueError extends Error {
  constructor(type, message) {
    super(message);
    this.type = type;
  }
}

MessageQueueError.ERROR_EMPTY_CONTENT = Symbol("empty_content");
MessageQueueError.ERROR_HANDLE = Symbol("handle_error");
MessageQueueError.ERROR_CONNECT = Symbol('connect_error');
MessageQueueError.ERROR_BORROW = Symbol('borrow_error');
MessageQueueError.ERROR_RETURN = Symbol('return_error');
MessageQueueError.ERROR_REGISTER = Symbol('register_error');

module.exports = MessageQueueError;
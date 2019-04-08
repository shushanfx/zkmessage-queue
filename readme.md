## ZKMessage Queue

A distribution message queue based on zookeeper.

## How to use.

```bash
npm install zkmessage-queue --save
```

```javascript
const messageService = new MessageQueue({
  servers: '10.138.30.208:2181',
  path: '/zkconfig/video_dev',
  username: 'zkconfig',
  password: 'zkconfig',
  handle: async (messageBean, done) => {
    // handle message
    logger.info('Handle message: ', messageBean);
    setTimeout(() => {
      let random = Math.floor(Math.random() * 100);
      if (random < 10) {
        done(new Error('Handle fail.'));
      } else {
        done();
      }
    }, 1);
  },
  handleError: async (messageBean, done) => {
    // 处理错误消息
    logger.info('Handle error message: ', messageBean);
    done();
  }
});
messageService.connect();
```

> PS: call method `handle` to handle message, if an error is thrown, `handleError` message will be called. Please make sure `done` method must be called after handling operation, or the proess will be truggled in this message forever.

> Some things you must be noticed.
> 1 The username must have the read and write previleges of the path. At this point only `digest` and `anonymous` authentication are supported.
> 2 This path must exist: `${path}/message`，the path to store mesage queue, `${path}/pedding`，the path to store pedding message， `${path}/queue`, the path to queue the register process.

## API

- constructor(options)  
  The options config is as follows:
  - servers: the servers of zookeeper servers, separated by `,`, such as `10.138.30.208:2181`;
  - path: the path to store message and connection queue;
  - username: the username for zookeeper;
  - passport: the passport for zookeeper;
  * charset: the charset for message content, default `utf-8`;
  * handleRetryMaxTimes: the max retry times for one message, default is `10`;
  * registerRetryMaxTimes: the max retry times for register this process to zookeeper queue, after retry for `${registerRetryMaxTimes}` times of retry, this process is discast forever until an restart operation is taken. default is `200`
  * borrowRetryTimes: the max retry times for borrowing a message from message queue to pedding queue. default is `2`;
  * returnRetryTimes: the max retry times for returning a message from pedding queue to message queue. default is `2`;
  * messageCacheTriggerCount: the trigger count for message queue to switch cache hint on, which in other words, if the length of message queue is bigger than this value, the cache will switch on automatically. default is `100`,
  * messageCacheMaxCount: the max size of cache. which means how many messages can cache store. default is `50`;
  * messageCacheRetryTimes: the max retry times for save cache, default is `2`;
  * handle: A method to handle the consumed message.
    - messageObject, an instance of `MessageBean`;
    - done, a callback function which must be called after the handing operation. You can pass an error parameter to the function to indicate the a failure.
  * handleError: A method to handle the error message.
    - messageObject, an instance of `MessageBean`;
- appendMessage(id, message, callback), append a message to message queue. the done method will be be called while appending success or fail.
  - id: String, the id of the message, please make this an uniq.
  - message: String, the message content.
  - callback: callback function, (Error, messageBean), an error parameter will be taken if an error has been thrown.

* returnPeddingMessages(done), return the pedding messages to the message queue.
  - callback: callback function, (Error).

- connect(callback)
  Connect to a zookeeper server, after connected, the callback method will be called.
  - callback(), a method with no parameters.
- close()
  Close the connection.

eg:

```javascript
const MessageQueue = require('zkmessage-queue');
const uuid = require('uuid');

const queue = new MessageQueue(options);

queue.appendMessage(uuid(), 'A test message', (err, messageBean) => {
  if (err) {
    console.error(err);
  } else {
    console.info(messageBean);
  }
});
```

## Operations

There are three ways to change the message queue:

1. Append operation by calling the `appendMessage` method.
2. Return pedding message to message queue by calling the `returnPeddingMessages`.
3. Consume operation automatically, which the `handle` method will be called.

## Events

Some event will be trigged, the event list is as follow:

- EVENT_CONNECT_SUCCESS, fired when connect successfully.
- EVENT_CONNECT_ERROR, fired when connect fail.
- EVENT_ERROR, fire an error occor.
- EVENT_INCOMING_MESSAGE, fired when a message is appended successfully.
- EVENT_HANDLE_MESSAGE, fired when a message is handled successfully.
- EVENT_HANDLE_MESSAGE_ERROR, fired after `handleError` has been called.
- EVENT_REMOVE_MESSAGE, fired when a message has been removed successfully(delete from pedding queue).

You can bind an event handler like this:

```javascript
const MessageQueue = require('zkmessage-queue');

let queue = new MessageQueue(...)
queue.on(MessageQueue.EVENT_CONNECT_SUCESS)
```

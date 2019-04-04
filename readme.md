## ZKMessage Queue

A distribution message queue.

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

> PS: call method `handle` to handle message, if an error is thrown, `handleError` message will be called. Please make sure after handling the `done` method must be called, or the proess will not handle other message task any more.

> Some thing you must be noticed.
> 1 The username must have the read and write previleges of the path.
> 2 This path must exist: `${path}/message`，the path to store mesage queue, `${path}/pedding`，the path to

## API

- constructor(options)  
  The options config is as follows:
  - servers: the servers of zookeeper servers, separated by `,`, such as 10.138.30.208:2181;
  - path: the path to store message and connection queue.
  - username: the username for zookeeper.
  - passport: the passport for zookeeper.
- appendMessage(id, message, done), append a message to message queue. the done method will be be called while appending success or fail.
  - id: String, the id of the message, please make this an uniq.
  - message: String the message content.
  - done: callback function, (Error, messageBean)
- connect()
- close()

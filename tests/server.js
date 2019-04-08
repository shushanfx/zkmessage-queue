const Koa = require('koa');
const KoaRouter = require('koa-router');
const uuid = require('uuid');
const log4js = require('log4js');
const IS_TEST = process.env.NODE_ENV === 'test';

if (!IS_TEST) {
  log4js.configure({
    "appenders": {
      "default": {
        "type": "stdout"
      }
    },
    "categories": {
      "default": {
        "appenders": ["default"],
        "level": "debug"
      }
    }
  })
} else {
  log4js.configure({
    "appenders": {
      "default": {
        "type": "stdout"
      }
    },
    "categories": {
      "default": {
        "appenders": ["default"],
        "level": "debug"
      }
    },
    pm2: true,
    pm2InstanceVar: 'INSTANCE_ID'
  });
}

const logger = log4js.getLogger('server');
const MessageQueue = require('../src/message-queue');
const messageService = new MessageQueue({
  servers: "10.138.30.208:2181",
  path: "/zkconfig/video_dev",
  username: "zkconfig",
  password: "zkconfig",
  handle: async (messageBean, done) => {
    // handle message
    logger.info('Handle message: ', messageBean);
    setTimeout(() => {
      let random = Math.floor(Math.random() * 100);
      if (random < 1) {
        done(new Error('Handle fail.'));
      } else {
        done();
      }
    }, 10000);
  },
  handleError: async (messageBean, done) => {
    // 处理错误消息
    logger.info('Handle error message: ', messageBean);
    done();
  }
});
messageService.on(MessageQueue.EVENT_CONNECT_SUCCESS, () => {
  logger.info('连接成功!');
})
messageService.connect();

const app = new Koa();
const router = new KoaRouter();
router.get('/notify-test', async (ctx) => {
  let id = uuid();
  let message = `This is a test message ${id}`;
  ctx.body = await new Promise(resolve => {
    messageService.appendMessage(id, message, (err) => {
      if (err) {
        resolve({
          code: -1,
          message: err.message
        });
      } else {
        resolve({
          code: 1
        })
      }
    })
  });
});
router.get('/notify-return', async (ctx) => {
  await new Promise((resolve) => {
    messageService.returnPeddingMessages(resolve);
  });
  ctx.body = {
    success: 1
  };
});

app.use(router.allowedMethods()).use(router.routes());
const server = app.listen(9204, () => {
  let address = server.address();
  logger.info("Server start at %s %s", address.address, address.port);
})
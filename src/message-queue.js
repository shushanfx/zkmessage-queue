const zookeeper = require('node-zookeeper-client');
const EventEmitter = require('events').EventEmitter;
const logger = require('log4js').getLogger('messageQueue');
const ip = require('ip');
const uuid = require('uuid');
const SafeObject = require("./object-utils");
const MessageBean = require("./message-bean");
const MessageUtils = require('./message-utils');

class MessageQueue extends EventEmitter {
  constructor(options) {
    super()
    this.options = Object.assign({
      scheme: "digest",
      connectMaxRetryTimes: 100,
      charset: 'utf8',
      handleRetryMaxTimes: 10,
      registerRetryMaxTimes: 200,
      borrowRetryTimes: 2,
      returnRetryTime: 2
    }, options);
    this.path = this.options.path;
    this.messagePath = this.options.path + '/message'
    this.peddingPath = this.options.path + '/pedding';
    this.queuePath = this.options.path + '/queue';

    this.queueCount = 0;
    this.messageWatcher = null;
    this.queueWatcher = null;

    this.username = this.options.username;
    this.password = this.options.password;
    this.scheme = this.options.scheme || "digest";
    this.servers = this.options.servers;

    this.isConnecting = false;
    this.isConnected = false;
    this.isShutdown = false;
    this.connectRetryTimes = 0;
    this.connectionID = uuid();
    this.queueID = null;
    this.connectionInfo = {
      id: this.connectionID,
      ip: ip.address(),
      createTime: new Date(),
      connectTime: null, // 连接时间
      count: 0, // 已经处理任务数据
      registerTime: null,
      handleTime: null
    };
    this.connectionCallbacks = [];
    this.unregisterRetryTimes = 0;
    this.registerRetryTimes = 0;
    this.queueRetryTimes = 0;
    this.registerRetryMaxTimes = this.options.registerRetryMaxTimes;
    this.isHandling = false;
  }
  connect(callback) {
    if (typeof callback === 'function') {
      this.connectionCallbacks.push(callback);
    }
    if (!this.isConnected) {
      if (!this.isConnecting) {
        this.isConnecting = true;
        this.client = zookeeper.createClient(this.servers, this.options);
        this._bindEvent();
        this._connect();
      }
    } else {
      this.handleConnectionCallback();
    }
  }
  handleConnectionCallback() {
    let callback = this.connectionCallbacks.shift();
    while (callback) {
      callback.call(this);
      callback = this.connectionCallbacks.shift();
    }
  }
  _bindEvent() {
    var me = this;
    this.client.on("state", function (state) {
      if (state === zookeeper.State.SYNC_CONNECTED) {
        me.connectionInfo.connectTime = new Date();
        me.isConnecting = true;
        me.isConnected = true;
        me.isShutdown = false;
        me._auth();
        // 将连接注入队列
        me.register();
        me.emit(MessageQueue.EVENT_CONNECT_SUCCESS);
      } else if (state === zookeeper.State.DISCONNECTED ||
        state === zookeeper.State.EXPIRED) {
        me._connect();
      } else if (state === zookeeper.State.AUTH_FAILED) {
        // 认证失败，不在重试
        me.emit(MessageQueue.EVENT_ERROR, new Error("auth failed"));
      } else if (state === -2) {
        // close
        me.emit(MessageQueue.EVENT_CLOSE);
        me.isConnected = false;
        me.isConnecting = false;
        me.isShutdown = true;
        me.client = null;
      }
    });
  }
  _connect() {
    if (!this.isShutdown) {
      this.connectRetryTimes++;
      this.client.connect();
    }
  }
  _auth() {
    if (this.username) {
      this.client.addAuthInfo(this.scheme,
        Buffer.from([this.username, this.password].join(":")));
    }
  }
  close() {
    this.isShutdown = true;
    if (this.client != null) {
      this.client.close();
    }
  }
  handlePedding(realID, messageObject) {
    let handler = this.options.handle;
    let handlerError = this.options.handleError;
    let start = Date.now();
    messageObject.realID = realID;
    if (messageObject.retryTimes >= this.options.handleRetryMaxTimes) {
      // 抓取失败
      try {
        if (handlerError) {
          handlerError.call(this, messageObject, () => {
            this.deleteMessage(realID, () => {
              this.emit(MessageQueue.EVENT_HANDLE_MESSAGE_ERROR, messageObject);
            });
          });
        } else {
          this.deleteMessage(realID, () => {
            this.emit(MessageQueue.EVENT_HANDLE_MESSAGE_ERROR, messageObject);
          });
        }
      } catch (e) {
        this.deleteMessage(realID, () => {
          this.emit(MessageQueue.EVENT_HANDLE_MESSAGE_ERROR, messageObject);
        });
        logger.error('Current thread handle error fail for message %s due to %s', realID, e);
      }
      return;
    }
    if (handler) {
      try {
        logger.debug('Current thread is handing message %s', realID);
        this.emit(MessageQueue.EVENT_HANDLE_MESSAGE, messageObject);
        handler.call(this, messageObject, (err) => {
          if (err) {
            logger.error('Handle message error %s', err);
            messageObject.retryTimes++;
            this.returnMessage(realID, messageObject);
          } else {
            this.deleteMessage(realID, () => {
              logger.debug('Handle message success, cost: %d', Date.now() - start);
              this.emit(MessageQueue.EVENT_HANDLE_MESSAGE_SUCCESS, messageObject);
            });
          }
        });
      } catch (e) {
        // 处理消息失败，则交给其他人处理
        logger.error("Handle message error.", e);
        messageObject.retryTimes++;
        this.returnMessage(realID, messageObject);
      }
    } else {
      logger.debug('Current thread has no handle for message %s', realID);
      messageObject.retryTimes++;
      this.returnMessage(realID, messageObject);
    }
  }
  borrowMessage(realID, done, iCount = 0) {
    if (iCount >= this.options.borrowRetryTimes) {
      this.unregister(this.queueID, true, () => {
        this.isHandling = false;
        done && done(new Error("Expires borrow retry time."));
      });
      return;
    }
    let oldPath = `${this.messagePath}/${realID}`;
    let newPath = `${this.peddingPath}/${realID}`;
    logger.debug('Borrow message from %s to %s', oldPath, newPath);
    // 加入pedding
    this.client.getData(oldPath, (err, data, stat) => {
      if (err) {
        // 无法borrow，重试
        logger.error('Borrow message content error due to %s', err);
        this.borrowMessage(realID, done, iCount + 1);
        return;
      }
      try {
        let json = SafeObject.parse(data.toString(this.options.charset));
        json.startHandleTimestamp = Date.now();
        if (json && json.id && json.content) {
          let transaction = this.client.transaction();
          transaction.remove(oldPath, stat.version).create(newPath, data).commit((err) => {
            if (err) {
              // 处理失败，重试一次
              logger.error('Borrow message to pedding fail due to %s', err);
              this.borrowMessage(realID, done, iCount + 1);
              return;
            }
            logger.debug('Borrow message success: %s', newPath);
            this.unregister(this.queueID, false, () => {
              done && done(null, realID, json);
            });
          });
        } else {
          this.client.remove(oldPath, stat.version, (err) => {
            if (err) {
              logger.error('Borrow message illegal, drop it error due to %s', err);
            }
            this.unregister(this.queueID, true, () => {
              this.isHandling = false;
            });
          });
        }
      } catch (e) {
        // 处理失败，让出权限
        this.unregister(this.queueID, true, () => {
          this.isHandling = false;
          done && done(e);
        });
        logger.error('Borrow message content error due to %s', e);
      }
    });
  }
  returnMessage(realID, messageBean, done = null, iCount = 0) {
    if (iCount >= 2) {
      // 最多重试两次，如果还是不行，则放弃治疗
      this.register(() => {
        this.isHandling = false;
        done && done(new Error('Return message retry time expires.'));
      });
      return;
    }
    let oldPath = `${this.peddingPath}/${realID}`;
    let newPath = `${this.messagePath}/m`;
    logger.debug('Return message from %s to %s, times: %d', oldPath, newPath, iCount);

    this.client.transaction()
      .remove(oldPath)
      .create(newPath,
        Buffer.from(SafeObject.stringify(messageBean), this.options.charset),
        zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL)
      .commit((err) => {
        if (err) {
          // 操作失败;
          logger.error('Return messsage fail due to %s.', err);
          this.returnMessage(realID, messageBean, done, iCount + 1)
        } else {
          logger.debug('Return message success');
          this.register(() => {
            this.isHandling = false;
            done && done();
          });
        }
      });
  }
  deleteMessage(realID, done = null, iCount = 0) {
    if (iCount >= 2) {
      // 最多重试两次，如果还是不行，则放弃治疗
      this.register(() => {
        this.isHandling = false;
        done && done(new Error('Delete message retry time expires.'));
      });
      return;
    }
    let newPath = `${this.peddingPath}/${realID}`;
    logger.debug('Delete message %s', newPath);
    this.client.remove(newPath, (err) => {
      if (err) {
        this.deleteMessage(realID, done, iCount++);
      } else {
        // 消费消息完毕，重新可以接受消息
        logger.debug('Delete message %s success', newPath);
        this.connectionInfo.count++;
        this.emit(MessageQueue.EVENT_REMOVE_MESSAGE, realID);
        this.register(() => {
          this.isHandling = false;
          done && done();
        });
      }
    });
  }
  appendMessage(id, message, done, iCount = 0) {
    if (typeof message !== 'string') {
      logger.error('Append message fail due message is not a string value.');
      done && done(new Error("Only string message can be append."));
      return;
    }
    if (iCount >= 3) {
      logger.error('Append message fail.');
      done && done(new Error('Append message fail.'));
      return;
    }
    let start = Date.now();
    let messageBean = MessageBean.create(id, message, null);
    this.client.create(`${this.messagePath}/m`,
      Buffer.from(SafeObject.stringify(messageBean), this.options.charset),
      zookeeper.CreateMode.PERSISTENT_SEQUENTIAL,
      (err, result) => {
        if (err) {
          logger.error('Append message error %s.', err);
          this.appendMessage(id, message, done, iCount++);
        } else {
          logger.debug('Append message sucess %s, cost: %d', result, Date.now() - start);
          messageBean.fullPath = result;
          this.emit(MessageQueue.EVENT_INCOME_MESSAGE, messageBean);
          done && done(null, messageBean);
        }
      })
  }
  handleMessage() {
    if (this.isConnected) {
      // logger.debug('Watch message in %s', this.messagePath);
      if (this.isHandling) {
        return;
      }
      this.isHandling = true;
      let path = this.messagePath;
      this.client.getChildren(path, (err, list) => {
        if (err) {
          // 让出权限
          logger.error('Handle message children error due to %s', err);
          this.unregister(this.queueID, true, () => {
            this.isHandling = false;
          });
        } else if (list && list.length) {
          // 处理消息
          let item = MessageUtils.getMin(list);
          logger.debug('Message count: %d', list.length);
          this.borrowMessage(item, (err, realID, message) => {
            if (!err) {
              logger.debug('Current thread is pedding message %s', item);
              this.handlePedding(realID, message);
            }
          })
        } else {
          this.isHandling = false;
          this.registerMessageWatch();
        }
      })
    }
  }
  handleQueue() {
    // 注册watch，包括两类
    this.getQueue((list, position) => {
      logger.debug("Current postion: %d/%d", position + 1, list.length);
      // 处于第0的位置
      if (position === 0) {
        // 有权处理消息
        logger.debug('Current thread is in first position, begin to handle message.');
        this.handleMessage();
      } else if (position > 0) {
        // 检查前辈是否存在
        logger.debug('Current thread is not in first position, register watch for %s', list[position - 1]);
        this.registerQueueWatch(list[position - 1]);
      } else {
        // 不应该出现此类情况，直接放弃
        process.exit(1);
      }
    })
  }
  registerQueueWatch(prevID) {
    if (this.isConnected && prevID) {
      let path = `${this.queuePath}/${prevID}`;
      this.client.exists(path, (event) => {
        logger.debug('Queue change event %s:%s', event.getName(), event.getPath());
        this.handleQueue();
      }, (err, exist) => {
        if (err) {
          logger.error('Fail to check children of %s due to %s.', path, err);
          this.handleQueue();
        } else if (!exist) {
          // 元素不存在，获取queue列表
          logger.error('Register queue watch fail because of not exits.%s', exist);
          this.handleQueue();
        } else {
          logger.debug('Register queue watch success: %s', path);
        }
      })
    }
  }
  registerMessageWatch() {
    if (this.isConnected) {
      let path = this.messagePath;
      let arr = [path];
      if (!this.messageWatcher) {
        this.messageWatcher = (event) => {
          logger.debug('Handle message event %s: %s', event.getName(), event.getPath());
          this.messageWatcher = null;
          this.handleMessage();
        }
        arr.push(this.messageWatcher);
      }
      arr.push(() => {
        logger.debug('Register message watch success: %s', this.messagePath);
      })
      this.client.getChildren.apply(this.client, arr);
    }
  }
  returnPeddingMessages(callback) {
    if (this.client) {
      let path = this.peddingPath;
      this.client.getChildren(path, (err, list) => {
        let iCount = 0;
        let func = (i) => {
          if (list && list.length > 0 && i < list.length) {
            let item = list[i];
            let newPath = `${path}/${item}`;
            this.client.getData(newPath, (err, data) => {
              let json = null;
              if (data) {
                json = SafeObject.parse(data.toString('utf-8'));
              }
              if (json && json.id && json.content) {
                // 重置retryTimes
                json.retryTimes = 0;
                let transaction = this.client.transaction();
                transaction.remove(newPath).create(`${this.messagePath}/${json.id}-`, Buffer.from(SafeObject.stringify(json), 'utf-8'), zookeeper.CreateMode.PERSISTENT_SEQUENTIAL)
                  .commit((err) => {
                    if (!err) {
                      iCount++;
                    }
                    func(i + 1);
                  });
              } else {
                func(i + 1);
              }
            });
          } else {
            logger.debug('Return pedding %d/%d', iCount, (list && list.length > 0 ? list.length : 0));
            callback && callback();
          }
        }
        func(0);
      });
    }
  }
  getQueue(done) {
    if (this.isConnected) {
      let path = this.queuePath;
      this.client.getChildren(path, (err, list) => {
        if (err) {
          logger.error('Get queue fail due to %s, retry times: %s', err, this.queueRetryTimes);
          this.queueRetryTimes++;
          setTimeout(() => {
            this.getQueue(done);
          }, this.queueRetryTimes * 100);
        } else {
          this.queueRetryTimes = 0;
          let newList = MessageUtils.sort(list);
          let newPosition = MessageUtils.position(newList, this.connectionID);
          done(newList, newPosition);
        }
      });
    }
  }
  unregister(queueID, isRegister = false, done) {
    if (this.isConnected && queueID) {
      logger.debug('Current thread unregister: %s', queueID);
      let transaction = this.client.transaction();
      transaction.remove(queueID)
      if (isRegister) {
        let queuePath = this.queuePath;
        let id = this.connectionID;
        let path = `${queuePath}/${id}`;
        transaction.create(path, Buffer.from(JSON.stringify(this.connectionInfo), 'utf8'), zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL);
      }
      transaction.commit((err, results) => {
        if (err) {
          this.unregisterRetryTimes++;
          logger.error('清除队列节点【%s】失败，【%s】， 系统将重试，重试次数：' + this.unregisterRetryTimes, queueID, err);
          logger.error(err);
          setTimeout(() => {
            this.unregister(queueID, isRegister, done);
          }, 100 * this.unregisterRetryTimes);
        } else {
          logger.debug('Current thread unregister success.');
          if (isRegister) {
            this.connectionInfo.registerTime = new Date();
            this.queueID = results[results.length - 1].path;
            logger.debug('Current thread register success: %s', this.queueID);
          } else {
            this.queueID = null;
          }
          done && done();
          if (isRegister) {
            this.handleQueue();
          }
        }
      });
    }
  }
  register(done) {
    if (this.isConnected &&
      this.registerRetryTimes <= this.registerRetryMaxTimes) {
      let queuePath = this.queuePath;
      let id = this.connectionID;
      let path = `${queuePath}/${id}`;
      logger.debug('Current thread register queue: %s', path);
      this.client.create(path, Buffer.from(SafeObject.stringify(this.connectionInfo), 'utf8'),
        zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL, (err, result) => {
          if (err) {
            this.registerRetryTimes++;
            logger.error('Current thread register fial, retry times: ' + this.registerRetryTimes, err);
            setTimeout(() => {
              this.register();
            }, 1000 * this.registerRetryTimes);
          } else {
            logger.debug('Current thread register success: %s', result);
            this.queueID = result;
            this.registerRetryTimes = 0;
            this.connectionInfo.registerTime = new Date();
            done && done();
            // 注册成功 处理队列
            this.handleQueue();
          }
        });
    }
  }
}

MessageQueue.EVENT_CONNECT_SUCCESS = Symbol('connectsuccess');
MessageQueue.EVENT_CONNECT_ERROR = Symbol('connecterror');
MessageQueue.EVENT_ERROR = Symbol('error');
MessageQueue.EVENT_INCOME_MESSAGE = Symbol('newmessage');
MessageQueue.EVENT_HANDLE_MESSAGE = Symbol('handlemessage');
MessageQueue.EVENT_HANDLE_MESSAGE_ERROR = Symbol('handlemessageerror');
MessageQueue.EVENT_REMOVE_MESSAGE = Symbol('removemessage');

module.exports = MessageQueue;
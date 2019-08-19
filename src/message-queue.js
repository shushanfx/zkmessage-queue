const zookeeper = require('node-zookeeper-client');
const logger = require('log4js').getLogger('messageQueue');
const ip = require('ip');
const uuid = require('uuid');

const AbstractMessageQueue = require('./abstract-message-queue');
const SafeObject = require("./object-utils");
const MessageBean = require("./message-bean");
const MessageUtils = require('./message-utils');

const ASYNC_FUNCTION = async function () {};
const EMPTY_FUNCTION = function () {};

const isAsyncFunction = (fun) => {
  if (typeof fun === 'function') {
    if (fun.constructor && fun.constructor.name === ASYNC_FUNCTION.constructor.name) {
      return true;
    }
  }
  return false;
}

class MessageQueue extends AbstractMessageQueue {
  /**
   * Create a new instance
   * @param {String} servers The zookeeper servers, split by `,`.
   * @param {String} path The path of message, such as `/zkmessage-queue/dev`.
   * @param {Function} handle a normal function or an asnyc function, which handle the message operation.
   * @param {Function} handleError a normal function or an async function, which handle the error message.
   * @param {String} [scheme] The type of auth check, default is schema.
   * @param {String} [username] the username of the auth.
   * @param {String} [password] the password of the auth.
   * @param {String} [chraset] the charset of the content. default is `utf-8`.
   * @param {Boolean} [checkExist] whether to check the existance of the path. default is `false`
   * @param {Number} [maxConnectRetryTimes] the maxium times of connecting retry. default is `100`
   * @param {Number} [maxRegisterRetryTimes] the maxium times of registering retry. default is `200`
   * @param {Number} [maxBorrowRetryTimes] the maxium times of borrowing message from the message queue. default is `2`
   * @param {Number} [maxReturnRetryTimes] the maxium times of returnning message to the mssage queue. default is `2`
   * @param {Number} [maxHandleRetryTimes] the maxium times of handling message, while an error is thrown. default is `2`
   * @param {Number} [cacheTriggerCount] the number of messages to trigger cache. default is `100`, 
   *  which means system will trigger cache automatically when the size of message queue reach this number.
   * @param {Number} [maxCacheSize] the max size of messages that cache holds. default is `50`.
   * @param {Number} [maxCacheRetryTimes] the max retry times for saving the cache. default is `2`.
   */
  constructor(options) {
    super()
    this.options = Object.assign({
      scheme: "digest",
      charset: 'utf8',
      maxConnectRetryTimes: 100,
      maxHandleRetryTimes: 2,
      maxRegisterRetryTimes: 200,
      maxBorrowRetryTimes: 2,
      maxReturnRetryTimes: 2,
      cacheTriggerCount: 100,
      maxCacheSize: 50,
      messageCacheRetryTimes: 2,
      checkExist: true
    }, options);
    if (typeof this.options.path !== 'string') {
      return new AbstractMessageQueue();
    }
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
    this.maxRegisterRetryTimes = this.options.maxRegisterRetryTimes;
    this.isHandling = false;
    this.messageCount = 0;
    this.isMessageCacheOn = this.options.cacheTriggerCount > 0 ? true : false;
    this.isInit = false;
  }
  connect(callback) {
    if (typeof callback === 'function') {
      this.connectionCallbacks.push(callback);
    }
    if (!this.isConnected) {
      if (!this.isConnecting) {
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
        me.isConnecting = false;
        me.isConnected = true;
        me.isShutdown = false;
        me._auth();
        me.emit(MessageQueue.EVENT_CONNECT_SUCCESS);
      } else if (state === zookeeper.State.DISCONNECTED ||
        state === zookeeper.State.EXPIRED) {
        this.isConnecting = false;
        this.isConnected = false;
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
    if (!this.isShutdown && !this.isConnecting) {
      this.isConnecting = true;
      this.connectRetryTimes++;
      this.client.connect();
      logger.debug('Zkconfig is connecting.');
    }
  }
  _auth() {
    if (this.username) {
      this.client.addAuthInfo(this.scheme,
        Buffer.from([this.username, this.password].join(":")));
    }
    this._generate();
  }
  _generate() {
    if (!this.isInit && this.options.checkExist) {
      let promiseGenerator = (path) => {
        return new Promise((resolve) => {
          this.client.exists(path, (_, stats) => {
            resolve(!!stats);
          });
        }).then((isExist) => {
          logger.debug('Generator path %s', path);
          if (!isExist) {
            return new Promise((resolve) => {
              this.client.mkdirp(path, (e) => {
                resolve(path);
              })
            })
          }
          return true;
        });
      }
      promiseGenerator(this.messagePath)
        .then(promiseGenerator(this.peddingPath))
        .then(promiseGenerator(this.queuePath))
        .then(() => {
          this.isInit = true;
          this.register();
        });
    } else {
      this.register();
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
    if (messageObject.retryTimes >= this.options.maxHandleRetryTimes) {
      // 抓取失败
      try {
        if (typeof handlerError === 'function') {
          if (isAsyncFunction(handlerError)) {
            handlerError.call(this, messageObject, EMPTY_FUNCTION).then(() => {
              this.deleteMessage(realID, () => {
                this.emit(MessageQueue.EVENT_HANDLE_MESSAGE_ERROR, messageObject);
              });
            }).catch(() => {
              this.deleteMessage(realID, () => {
                this.emit(MessageQueue.EVENT_HANDLE_MESSAGE_ERROR, messageObject);
              });
            });
          } else {
            handlerError.call(this, messageObject, () => {
              this.deleteMessage(realID, () => {
                this.emit(MessageQueue.EVENT_HANDLE_MESSAGE_ERROR, messageObject);
              });
            });
          }
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
    if (typeof handler === 'function') {
      try {
        logger.debug('Current thread is handing message %s', realID);
        this.emit(MessageQueue.EVENT_HANDLE_MESSAGE, messageObject);
        if (isAsyncFunction(handler)) {
          handler.call(this, messageObject, EMPTY_FUNCTION).then(() => {
            this.deleteMessage(realID, () => {
              logger.debug('Handle message success, cost: %d', Date.now() - start);
              this.emit(MessageQueue.EVENT_HANDLE_MESSAGE_SUCCESS, messageObject);
            });
          }).catch(e => {
            if (e) {
              logger.error('Handle message error %s', e);
            }
            messageObject.retryTimes++;
            this.returnMessage(realID, messageObject);
          })
        } else {
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
        }
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
    if (iCount >= this.options.maxBorrowRetryTimes) {
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
    if (iCount >= this.options.maxReturnRetryTimes) {
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
        logger.error('Delete message error due to %s', err);
        this.deleteMessage(realID, done, iCount + 1);
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
  handleMessageCache() {
    if (this.isConnected) {
      if (this.isHandling) {
        return;
      }
      if (!this.isMessageCacheOn) {
        this.handleMessage(true);
        return;
      }
      this.isHandling = true;
      this.client.getData(this.messagePath, (err, data, stat) => {
        if (err) {
          logger.error('Handle message cache error due to %s', err);
          this.unregister(this.queueID, true, () => {
            this.isHandling = false;
          });
        } else {
          let value = null;
          if (data && data.length) {
            value = SafeObject.parse(data.toString(this.options.charset));
          }
          this.messageCount = stat.numChildren;
          logger.debug('Message count: %d', this.messageCount);
          if (Array.isArray(value) && value.length > 0) {
            // 命中缓存
            logger.debug('Handle message cache hint, cache length %d', value.length);
            let id = value.shift();
            this.saveCache(value, (err) => {
              if (err) {
                this.isHandling = false;
                this.handleMessage(true);
              } else {
                this.borrowMessage(id, (err, realID, message) => {
                  if (!err) {
                    logger.debug('Current thread is pedding message %s', id);
                    this.handlePedding(realID, message);
                  } else {
                    // 操作失败，saveCache
                    this.saveCache([]);
                  }
                })
              }
            });
          } else {
            this.isHandling = false;
            this.handleMessage(true);
          }
        }
      })
    }
  }
  saveCache(cacheList, done, iCount = 0) {
    if (iCount >= this.options.messageCacheRetryTimes) {
      logger.error('Save cache error due to exceed.');
      done && done(new Error('Save cache exceed.'));
      return;
    }
    let path = this.messagePath;
    let content = Buffer.from(SafeObject.stringify(cacheList), this.options.charset);
    this.client.setData(path, content, (err) => {
      if (err) {
        // error.
        logger.error('Save cache error due to %s.', err);
        this.saveCache(cacheList, done, iCount + 1);
      } else {
        done && done();
      }
    });
  }
  handleMessage() {
    if (this.isConnected) {
      // logger.debug('Watch message in %s', this.messagePath);
      if (this.isHandling) {
        return;
      }
      this.isHandling = true;
      let path = this.messagePath;

      // 如果children的内容足够长时，getChildren操作将足够长，此处应该优化一下。
      let start = Date.now();
      this.client.getChildren(path, (err, list) => {
        if (err) {
          // 让出权限
          logger.error('Handle message children error due to %s', err);
          this.unregister(this.queueID, true, () => {
            this.isHandling = false;
          });
        } else if (list && list.length) {
          // 处理消息
          this.messageCount = list.length;
          logger.debug('Message count: %d, read cost: %d', list.length, Date.now() - start);
          let isOpenCache = this.messageCount > this.options.cacheTriggerCount &&
            this.options.cacheTriggerCount > 0;
          start = Date.now();
          if (isOpenCache) {
            let cacheList = MessageUtils.getTop(list, this.options.maxCacheSize);
            logger.debug('Message sort: %d, cost: %d', cacheList.length, Date.now() - start);
            let item = cacheList.shift();
            if (!this.isMessageCacheOn) {
              logger.debug('Handle message turn on cache.');
            }
            this.isMessageCacheOn = true;
            this.saveCache(cacheList, (err) => {
              if (err) {
                logger.error('Handle message children error due to %s', err);
                // 保存缓存失败
                this.unregister(this.queueID, true, () => {
                  this.isHandling = false;
                });
              } else {
                this.borrowMessage(item, (err, realID, message) => {
                  if (!err) {
                    logger.debug('Current thread is pedding message %s', item);
                    this.handlePedding(realID, message);
                  }
                })
              }
            });
          } else if (this.isMessageCacheOn) {
            // 之前开启过cache，但是条件不具备
            logger.debug('Handle message turn off cache.');
            this.saveCache([], (err) => {
              if (err) {
                logger.error('Handle message cache empty error due to %s', err);
                this.unregister(this.queueID, true, () => {
                  this.isHandling = false;
                });
              } else {
                this.isMessageCacheOn = false;
                let item = MessageUtils.getMin(list);
                this.borrowMessage(item, (err, realID, message) => {
                  if (!err) {
                    logger.debug('Current thread is pedding message %s', item);
                    this.handlePedding(realID, message);
                  }
                })
              }
            })
          } else {
            let item = MessageUtils.getMin(list);
            this.borrowMessage(item, (err, realID, message) => {
              if (!err) {
                logger.debug('Current thread is pedding message %s', item);
                this.handlePedding(realID, message);
              }
            })
          }

        } else {
          this.messageCount = 0;
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
        this.handleMessageCache();
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
      this.registerRetryTimes <= this.maxRegisterRetryTimes) {
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
  push(content, done) {
    if (content === null && content === undefined) {
      return typeof done === 'function' && done(new Error('Content can not be '))
    }
    let _content = null;
    if (typeof content === "object") {
      _content = SafeObject.stringify(content);
    } else {
      _content = content.toString();
    }
    let id = uuid();
    this.appendMessage(id, _content, done);
  }
  async pushPromise(content) {
    return new Promise((resolve, reject) => {
      this.push(content, (err, messageBean) => {
        if (err) {
          reject(err);
        } else {
          resolve(messageBean);
        }
      });
    });
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
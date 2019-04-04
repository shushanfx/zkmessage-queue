const SafeObject = require('./object-utils');

class Message {
  constructor() {
    this.id = null;
    this.realID = null;
    this.createTime = null;
    this.content = null;
    this.contentObject = null;
    this.retryTimes = 0;
  }
  encode() {
    this.contentObject = SafeObject.parse(this.content);
  }
  decode() {
    this.content = SafeObject.stringify(this.contentObject);
  }
}

Message.build = Message.create = (id, content, realID) => {
  let message = new Message();
  message.id = id;
  message.content = content;
  message.realID = realID;
  message.createTime = new Date();
  return message;
}

Message.from = (str) => {
  let obj = SafeObject.parse(str);
  let message = new Message();
  Object.keys(obj).forEach((item) => {
    message[item] = obj[item];
  });
  return message;
}

module.exports = Message;
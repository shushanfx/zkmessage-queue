const logger = require('log4js').getLogger('object-utils');

module.exports.parse = (str) => {
  if (typeof str === 'string' && str) {
    try {
      return JSON.parse(str);
    } catch (e) {
      logger.error(e);
    }
  }
  return null;
}

module.exports.stringify = (bean) => {
  if (typeof bean === 'object' && bean) {
    try {
      return JSON.stringify(bean);
    } catch (e) {
      logger.error(e);
    }
  } else if (typeof bean === 'number' && !Number.isNaN(bean)) {
    return bean.toString();
  } else if (typeof bean === 'boolean') {
    return bean.toString();
  } else if (bean) {
    return bean.toString();
  }
  return '';
}
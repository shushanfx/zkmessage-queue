const REG = /(\d{10})$/i;

const MessageUtils = {
  getMin(list) {
    let ret = null;
    let id = null;
    if (list && list.length > 0) {
      id = this.getID(list[0]);
      ret = list[0];
      for (let i = 1; i < list.length; i++) {
        let temp = this.getID(list[i]);
        if (temp >= 0 && temp < id) {
          id = temp;
          ret = list[i];
        }
      }
    }
    return ret;
  },
  getID(item) {
    if (item && typeof item === 'string') {
      let arr = REG.exec(item);
      if (arr && arr[1]) {
        return parseInt(arr[1], 10);
      }
    }
    return -1;
  },
  sort(list) {
    let newList = list.map(item => item);
    newList.sort((a, b) => {
      let id1 = this.getID(a);
      let id2 = this.getID(b);
      return id1 - id2;
    });
    return newList;
  },
  position(list, item) {
    for (let i = 0; i < list.length; i++) {
      let jtem = list[i];
      if (jtem && jtem.indexOf(item) === 0) {
        return i;
      }
    }
    return -1;
  }
}

module.exports = MessageUtils;
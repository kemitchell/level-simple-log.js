var asyncQueue = require('async.queue')
var collect = require('stream-collector')
var defined = require('defined')
var dezalgo = require('dezalgo')
var lexint = require('lexicographic-integer')
var pump = require('pump')
var through = require('through2')

module.exports = SimpleLog

function SimpleLog (levelup) {
  if (!(this instanceof SimpleLog)) {
    return new SimpleLog(levelup)
  }
  var self = this
  self._levelup = levelup
  self._head = false
  self._appendQueue = asyncQueue(function (task, done) {
    var entry = task.entry
    self.head(function (error, currentHead) {
      if (error) done(error)
      else {
        var newHead = currentHead + 1
        self.set(newHead, entry, function (error) {
          if (error) done(error)
          else {
            self._head = newHead
            done(null, newHead)
          }
        })
      }
    })
  })
}

SimpleLog.prototype.get = function (index, callback) {
  this._levelup.get(indexToKey(index), callback)
}

SimpleLog.prototype.set = function (index, entry, callback) {
  this._levelup.put(indexToKey(index), entry, callback)
}

SimpleLog.prototype.drop = function (index, callback) {
  this._levelup.del(indexToKey(index), callback)
}

SimpleLog.prototype.append = function (entry, callback) {
  this._appendQueue.push({entry: entry}, callback || noop)
}

SimpleLog.prototype.head = function (callback) {
  callback = dezalgo(callback)
  if (this._head !== false) callback(null, this._head)
  else {
    var self = this
    var keyStream = self._levelup.createKeyStream({
      reverse: true,
      limit: 1
    })
    collect(keyStream, function (error, keys) {
      if (error) callback(error)
      else
        if (keys.length === 0) {
          self._head = 0
          callback(null, 0)
        } else {
          var unpacked = lexint.unpack(keys[0], 'hex')
          callback(null, unpacked)
        }
    })
  }
}

SimpleLog.prototype.createStream = function (options) {
  options = options || {}
  return pump(
    this._levelup.createReadStream({
      gt: indexToKey(defined(options.from, 0)),
      limit: defined(options.limit, -1)
    }),
    through.obj(recordToEntry)
  )
}

SimpleLog.prototype.createReverseStream = function (options) {
  options = options || {}
  return pump(
    this._levelup.createReadStream({
      gt: indexToKey(defined(options.from, 0)),
      reverse: true,
      limit: defined(options.limit, -1)
    }),
    through.obj(recordToEntry)
  )
}

function recordToEntry (data, encoding, callback) {
  callback(null, {
    index: lexint.unpack(data.key, 'hex'),
    entry: data.value
  })
}

function indexToKey (index) {
  return (index === -1 ? '\xff' : lexint.pack(index, 'hex'))
}

function noop () { }

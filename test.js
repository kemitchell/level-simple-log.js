var SimpleLog = require('./')
var collect = require('stream-collector')
var encode = require('encoding-down')
var levelup = require('levelup')
var memdown = require('memdown')
var tape = require('tape')

var a = { a: 1 }
var b = { b: 2 }
var c = { c: 3 }

tape('append', function (test) {
  var log = testLog()
  log.append(a, function (error, index) {
    test.ifError(error, 'no error')
    test.equal(index, 1, 'first index is 1')
    test.end()
  })
})

tape('head', function (test) {
  var log = testLog()
  log.append(a, function () {
    log.head(function (error, head) {
      test.ifError(error, 'no error')
      test.equal(head, 1, 'head is 1')
      test.end()
    })
  })
})

tape('get', function (test) {
  var log = testLog()
  log.append(a, function () {
    log.get(1, function (error, entry) {
      test.ifError(error, 'no error')
      test.deepEqual(entry, a, 'yields new entry')
      test.end()
    })
  })
})

tape('set', function (test) {
  var log = testLog()
  log.append(a, function () {
    log.set(1, b, function (error) {
      test.ifError(error, 'no error')
      log.get(1, function (error, entry) {
        test.ifError(error, 'no error')
        test.deepEqual(entry, b, 'yields set entry')
        test.end()
      })
    })
  })
})

tape('drop', function (test) {
  var log = testLog()
  log.append(a, function () {
    log.append(b, function () {
      log.append(c, function () {
        log.drop(2, function (error) {
          test.ifError(error, 'no error')
          collect(log.createStream(), function (error, entries) {
            test.ifError(error, 'no error')
            test.deepEqual(
              entries,
              [{ index: 1, entry: a }, { index: 3, entry: c }]
            )
            test.end()
          })
        })
      })
    })
  })
})

tape('stream', function (test) {
  var log = testLog()
  log.append(a, function () {
    log.append(b, function () {
      collect(log.createStream(), function (error, entries) {
        test.ifError(error, 'no error')
        test.deepEqual(
          entries,
          [{ index: 1, entry: a }, { index: 2, entry: b }]
        )
        test.end()
      })
    })
  })
})

tape('stream to index', function (test) {
  var log = testLog()
  log.append(a, function () {
    log.append(b, function () {
      log.append(c, function () {
        collect(log.createStream({ from: 1 }), function (error, entries) {
          test.ifError(error, 'no error')
          test.deepEqual(
            entries,
            [{ index: 2, entry: b }, { index: 3, entry: c }]
          )
          test.end()
        })
      })
    })
  })
})

tape('stream with limit', function (test) {
  var log = testLog()
  log.append(a, function () {
    log.append(b, function () {
      log.append(c, function () {
        var options = { from: 1, limit: 1 }
        collect(log.createStream(options), function (error, entries) {
          test.ifError(error, 'no error')
          test.deepEqual(
            entries,
            [{ index: 2, entry: b }]
          )
          test.end()
        })
      })
    })
  })
})

tape('reverse stream', function (test) {
  var log = testLog()
  log.append(a, function () {
    log.append(b, function () {
      collect(log.createReverseStream(), function (error, entries) {
        test.ifError(error, 'no error')
        test.deepEqual(
          entries,
          [{ index: 2, entry: b }, { index: 1, entry: a }]
        )
        test.end()
      })
    })
  })
})

tape('reverse stream with limit', function (test) {
  var log = testLog()
  log.append(a, function () {
    log.append(b, function () {
      collect(
        log.createReverseStream({ from: 0, limit: 1 }),
        function (error, entries) {
          test.ifError(error, 'no error')
          test.deepEqual(
            entries,
            [{ index: 2, entry: b }]
          )
          test.end()
        }
      )
    })
  })
})

tape('reverse stream to index', function (test) {
  var log = testLog()
  log.append(a, function () {
    log.append(b, function () {
      log.append(c, function () {
        var options = { from: 1 }
        collect(
          log.createReverseStream(options),
          function (error, entries) {
            test.ifError(error, 'no error')
            test.deepEqual(
              entries,
              [{ index: 3, entry: c }, { index: 2, entry: b }]
            )
            test.end()
          }
        )
      })
    })
  })
})

function testLog () {
  return new SimpleLog(
    levelup(encode(memdown(), { valueEncoding: 'json' }))
  )
}

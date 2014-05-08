// Load Modules

var Lab = require('lab');
var Catbox = require('catbox');
var DynamoDB = require('..');

// Declare Internals

var internals = {
  options: {
    region: 'us-west-2',
    tableName: 'catbox-dynamodb-testing'
  },
  getTestCallbackID: {
    id: 'getTestCallback',
    segment: 'unitTests'
  },
  getTestInvalidID: {
    id: 'getTestInvalid',
    segment: 'unitTests'
  },
  getTestNotExistingID: {
    id: 'getTestNotExisting',
    segment: 'unitTests'
  },
  setTestCallbackID: {
    id: 'setTestCallback',
    segment: 'unitTests'
  },
  dropTestCallbackID: {
    id: 'dropTestCallback',
    segment: 'unitTests'
  },
  dropTestSuccessID: {
    id: 'dropTestSuccess',
    segment: 'unitTests'
  },
  dropTestNotExistingID: {
    id: 'dropTestNotExisting',
    segment: 'unitTests'
  },
  catboxID: {
    id: 'catbox',
    segment: 'integrationTests'
  },
  catboxShortID: {
    id: 'catboxShort',
    segment: 'integrationTests'
  }
};

Lab.experiment('Catbox-DynamoDB', {parallel: true}, function () {
  Lab.test('throws an error if not created with new', function (done) {
    var fn = function () {
      DynamoDB();
    };
    Lab.expect(fn).to.throw(Error);
    done();
  });

  Lab.test('sets the region to us-east-1 by defualt', function (done) {
    var dynamo = new DynamoDB();
    Lab.expect(dynamo).to.have.property('settings');
    Lab.expect(dynamo.settings).to.have.property('region');
    Lab.expect(dynamo.settings.region).to.equal('us-east-1');
    done();
  });

  Lab.test('locks the apiVersion to 2012-08-10 by default', function (done) {
    var dynamo = new DynamoDB();
    Lab.expect(dynamo).to.have.property('settings');
    Lab.expect(dynamo.settings).to.have.property('apiVersion');
    Lab.expect(dynamo.settings.apiVersion).to.equal('2012-08-10');
    done();
  });

  Lab.test('applies user specified options to settings', function (done) {
    var dynamo = new DynamoDB({region: 'us-east-2'});
    Lab.expect(dynamo).to.have.property('settings');
    Lab.expect(dynamo.settings).to.have.property('region');
    Lab.expect(dynamo.settings.region).to.equal('us-east-2');
    done();
  });

  Lab.experiment('#start', function () {

    Lab.test('exists', function (done) {
      var dynamo = new DynamoDB();
      Lab.expect(dynamo).to.have.property('start');
      done();
    });

    Lab.test('executes a callback', function (done) {
      var dynamo = new DynamoDB();
      dynamo.start(function () {
        done();
      });
    });

    Lab.test('creates a property "service"', function (done) {
      var dynamo = new DynamoDB();
      dynamo.start(function () {
        Lab.expect(dynamo).to.have.property('service');
        done();
      });
    });

    Lab.test('uses the user-generated settings to start the service', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        Lab.expect(dynamo.service.config.region).to.equal('us-west-2');
        done();
      });
    });
  });

  Lab.experiment('#stop', function () {
    Lab.test('exists', function (done) {
      var dynamo = new DynamoDB();
      Lab.expect(dynamo).to.have.property('stop');
      done();
    });

    Lab.test('deletes property service', function (done) {
      var dynamo = new DynamoDB();
      Lab.expect(dynamo).to.not.have.property('service');
      dynamo.start(function () {
        Lab.expect(dynamo).to.have.property('service');
        dynamo.stop();
        Lab.expect(dynamo).to.not.have.property('service');
        done();
      });
    });
  });

  Lab.experiment('#isReady', function () {
    Lab.test('exists', function (done) {
      var dynamo = new DynamoDB();
      Lab.expect(dynamo).to.have.property('isReady');
      done();
    });

    Lab.test('returns false when the service does not exist', function (done) {
      var dynamo = new DynamoDB();
      Lab.expect(dynamo.isReady()).to.be.false;
      done();
    });

    Lab.test('returns true when the service does exist', function (done) {
      var dynamo = new DynamoDB();
      dynamo.start(function () {
        Lab.expect(dynamo.isReady()).to.be.true;
        done();
      });
    });
  });

  Lab.experiment('#validateSegmentName', function () {
    Lab.test('exists', function (done) {
      var dynamo = new DynamoDB();
      Lab.expect(dynamo).to.have.property('validateSegmentName');
      done();
    });

    Lab.test('returns an error when the name is empty', function (done) {
      var dynamo, result;
      dynamo = new DynamoDB();
      result = dynamo.validateSegmentName();
      Lab.expect(result).to.be.instanceOf(Error);
      Lab.expect(result.message).to.be.equal('Empty string');
      done();
    });

    Lab.test('returns an error when the name has a null character', function (done) {
      var dynamo, result;
      dynamo = new DynamoDB();
      result = dynamo.validateSegmentName('\0test');
      Lab.expect(result).to.be.instanceOf(Error);
      Lab.expect(result.message).to.be.equal('Includes null character');
      done();
    });

    Lab.test('returns null when there are no errors', function (done) {
      var dynamo, result;
      dynamo = new DynamoDB();
      result = dynamo.validateSegmentName('valid');
      Lab.expect(result).to.not.be.instanceOf(Error);
      Lab.expect(result).to.not.exist;
      done();
    });
  });

  Lab.experiment('#get', function () {
    Lab.before(function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        dynamo.set(internals.getTestCallbackID, {test: 'getTestCallback'}, 10000, function () {
          var item = {
            value: {
              S: '"{"key": "value"'
            },
            stored: {
              N: Date.now().toString()
            },
            ttl: {
              N: '10000'
            }
          };
          item[dynamo.settings.hashAttribute] = {S: internals.getTestInvalidID.segment};
          item[dynamo.settings.rangeAttribute] = {S: internals.getTestInvalidID.id};

          dynamo.service.putItem({
            TableName: internals.options.tableName,
            Item: item
          }, function (err) {
            done();
          });
        });
      })
    });

    Lab.test('exists', function (done) {
      var dynamo = new DynamoDB();
      Lab.expect(dynamo).to.have.property('get');
      done();
    });

    Lab.test('executes a callback', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        dynamo.get(internals.getTestCallbackID, function (err, data) {
          done();
        });
      });
    });

    Lab.test('passes an error to the callback when the connection is closed', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.get(internals.getTestCallbackID, function (err, data) {
          Lab.expect(err).to.be.not.null;
          Lab.expect(err).to.be.instanceOf(Error);
          Lab.expect(err.message).to.equal('Connection not started');
          done();
      });
    });

    Lab.test('is able to retrieve an object thats stored when connection is started', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        dynamo.get(internals.getTestCallbackID, function (err, data) {
          Lab.expect(data.item.test).to.be.equal('getTestCallback');
          done();
        });
      });
    });

    Lab.test('returns error on invalid json being returned from dynamo', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        dynamo.get(internals.getTestInvalidID, function (err, data) {
          Lab.expect(err).to.be.not.null;
          Lab.expect(err).to.be.instanceOf(Error);
          Lab.expect(err.message).to.equal('Bad value content')
          done();
        });
      });
    });

    // 'returns not found on missing segment'

    Lab.test('passes a null item to the callback when the item doesn\'t exist', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        dynamo.get(internals.getTestNotExistingID, function (err, data) {
          Lab.expect(err).to.be.null;
          Lab.expect(data).to.be.null;
          done();
        });
      });
    });

  });

  Lab.experiment('#set', function () {
    Lab.test('exists', function (done) {
      var dynamo = new DynamoDB();
      Lab.expect(dynamo).to.have.property('set');
      done();
    });

    Lab.test('executes a callback', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        dynamo.set(internals.setTestCallbackID, {test: 'setTestCallback'}, 10000, function () {
          done();
        });
      });
    });

    Lab.test('passes an error to the callback when the connection is closed', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.set(internals.setTestCallbackID, {test: 'setTestCallback'}, 10000, function (err) {
        Lab.expect(err).to.be.not.null;
        Lab.expect(err).to.be.instanceOf(Error);
        Lab.expect(err.message).to.equal('Connection not started')
        done();
      });
    });

    Lab.test('adds an item to dynamo', {timeout: 3000}, function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        dynamo.set(internals.setTestCallbackID, {test: 'setTestCallback'}, 10000, function () {
          dynamo.get(internals.setTestCallbackID, function (err, data) {
            Lab.expect(data).to.exist;
            Lab.expect(data.item.test).to.equal('setTestCallback');
            done();
          });
        });
      });
    });

    Lab.test('doesn\'t return an error when the set succeeds', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        dynamo.set(internals.setTestCallbackID, {test: 'setTestCallback'}, 10000, function (err) {
          Lab.expect(err).to.not.exist;
          done();
        });
      });
    });

    Lab.test('passes an error to the callback when there is an error returned from setting an item', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        dynamo.set(internals.setTestCallbackID, {test: 'setTestCallback'}, 'number', function (err) {
          Lab.expect(err).to.exist
          done();
        });
      });
    });

    Lab.test('passes an error to the callback when there is a circular reference in the object', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        var obj = {a: 1};
        obj.b = obj;
        dynamo.set(internals.setTestCallbackID, obj, 10000, function (err) {
          Lab.expect(err).to.exist
          Lab.expect(err.message).to.equal('Converting circular structure to JSON');
          done();
        });
      });
    });

  });

  Lab.experiment('#drop', function () {
    Lab.before(function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        dynamo.set(internals.dropTestCallbackID, {test: 'dropTestCallback'}, 10000, function () {
          dynamo.set(internals.dropTestSuccessID, {test: 'dropTestSuccess'}, 10000, function () {
            done();
          });
        });
      });
    });

    Lab.test('exists', function (done) {
      var dynamo = new DynamoDB();
      Lab.expect(dynamo).to.have.property('drop');
      done();
    });

    Lab.test('executes a callback', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        dynamo.drop(internals.dropTestCallbackID, function (err, data) {
          done();
        });
      });
    });

    Lab.test('passes an error to the callback when the connection is closed', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.drop(internals.dropTestCallbackID, function (err, data) {
        Lab.expect(err).to.be.not.null;
        Lab.expect(err).to.be.instanceOf(Error);
        Lab.expect(err.message).to.equal('Connection not started')
        done();
      });
    });

    Lab.test('doesn\'t return an error when the drop succeeds', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        dynamo.drop(internals.dropTestSuccessID, function (err, data) {
          Lab.expect(err).to.not.exist;
          done();
        });
      });
    });

    Lab.test('passes an error to the callback when there is an error returned from dropping an item', function (done) {
      var dynamo = new DynamoDB(internals.options);
      dynamo.start(function () {
        dynamo.drop(internals.dropTestNotExistingID, function (err, data) {
          Lab.expect(err).to.exist;
          Lab.expect(err).to.be.instanceOf(Error);
          Lab.expect(err.message).to.equal('Item does not exist');
          done();
        });
      });
    });
  });

});

Lab.experiment('Catbox', {parallel: true}, function () {

  Lab.test('creates a new connection', function (done) {
    var client = new Catbox.Client(DynamoDB);
    client.start(function (err) {
      Lab.expect(client.isReady()).to.equal(true);
      done();
    });
  });

  Lab.test('closes the connection', function (done) {
    var client = new Catbox.Client(DynamoDB);
    client.start(function (err) {
      Lab.expect(client.isReady()).to.equal(true);
      client.stop();
      Lab.expect(client.isReady()).to.equal(false);
      done();
    });
  });

  Lab.test('gets an item after settig it', {timeout: 5000}, function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.start(function (err) {
      client.set(internals.catboxID, '123', 10000, function (err) {
        Lab.expect(err).to.not.exist;
        client.get(internals.catboxID, function (err, result) {
          Lab.expect(err).to.not.exist;
          Lab.expect(result.item).to.equal('123');
          done();
        });
      });
    });
  });

  Lab.test('fails setting an item circular references', function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.start(function (err) {
      var obj = {a: 1};
      obj.b = obj;
      client.set(internals.catboxID, obj, 10000, function (err) {
        Lab.expect(err.message).to.equal('Converting circular structure to JSON');
        done();
      });
    });
  });

  Lab.test('fails setting an item with very long ttl', function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.start(function (err) {
      client.set(internals.catboxID, '123', Math.pow(2, 31), function (err) {
        Lab.expect(err.message).to.equal('Invalid ttl (greater than 2147483647)');
        done();
      });
    });
  });

  Lab.test('ignored starting a connection twice on same event', function (done) {
    var client, x, start;
    client = new Catbox.Client(DynamoDB, internals.options);
    x = 2;
    start = function () {
      client.start(function (err) {
        Lab.expect(client.isReady()).to.be.true;
        --x;
        if (!x) {
          done();
        }
      });
    };
    start();
    start();
  });

  Lab.test('ignored starting a connection twice chained', function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.start(function (err) {
      Lab.expect(err).to.not.exist;
      Lab.expect(client.isReady()).to.be.true;
      client.start(function (err) {
        Lab.expect(err).to.not.exist;
        Lab.expect(client.isReady()).to.be.true;
        done();
      });
    });
  });

  Lab.test('returns not found on get when using null key', function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.start(function (err) {
      client.get(null, function (err, result) {
        Lab.expect(err).to.not.exist;
        Lab.expect(result).to.not.exist;
        done();
      });
    });
  });

  Lab.test('returns not found on get when item expired', function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.start(function (err) {
      client.set(internals.catboxShortID, '123', 1, function (err) {
        Lab.expect(err).to.not.exist;
        // Slightly cheating here
        // Assuming that the set && get operations take longer than 1ms combined
        client.get(internals.catboxShortID, function (err, result) {
          Lab.expect(err).to.not.exist;
          Lab.expect(result).to.not.exist;
          done();
        });
      });
    });
  });

  Lab.test('returns error on set when using null key', function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.start(function (err) {
      client.set(null, '123', 10000, function (err) {
        Lab.expect(err).to.exist;
        Lab.expect(err).to.be.instanceOf(Error);
        done();
      });
    });
  });

  Lab.test('returns error on get when using invalid key', function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.start(function (err) {
      client.get({}, function (err) {
        Lab.expect(err).to.exist;
        Lab.expect(err).to.be.instanceOf(Error);
        done();
      });
    });
  });

  Lab.test('returns error on drop when using invalid key', function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.start(function (err) {
      client.drop({}, function (err) {
        Lab.expect(err).to.exist;
        Lab.expect(err).to.be.instanceOf(Error);
        done();
      });
    });
  });

  Lab.test('returns error on set when using invalid key', function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.start(function (err) {
      client.set({}, {}, 10000, function (err) {
        Lab.expect(err).to.exist;
        Lab.expect(err).to.be.instanceOf(Error);
        done();
      });
    });
  });

  Lab.test('ignores set when using non-positive ttl value', function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.start(function (err) {
      client.set(internals.catboxID, {}, 0, function (err) {
        Lab.expect(err).to.not.exist;
        done();
      });
    });
  });

  Lab.test('returns error on drop when using null key', function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.start(function (err) {
      client.drop(null, function (err) {
        Lab.expect(err).to.exist;
        Lab.expect(err).to.be.instanceOf(Error);
        done();
      });
    });
  });

  Lab.test('returns error on get when stopped', function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.get(internals.catboxID, function (err, result) {
      Lab.expect(err).to.exist;
      done();
    });
  });

  Lab.test('returns error on set when stopped', function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.set(internals.catboxID, {}, 10000, function (err) {
      Lab.expect(err).to.exist;
      done();
    });
  });

  Lab.test('returns error on drop when stopped', function (done) {
    var client = new Catbox.Client(DynamoDB, internals.options);
    client.drop(internals.catboxID, function (err) {
      Lab.expect(err).to.exist;
      done();
    });
  });

  Lab.test('returns error on missing segment name', function (done) {
    var fn = function () {
      var client, cache;
      client = new Catbox.Client(DynamoDB, internals.options);
      cache = new Catbox.Policy({
        expiresIn: 10000
      }, client, '');
    }
    Lab.expect(fn).to.throw(Error);
    done();
  });

  Lab.test('returns error on bad segment name', function (done) {
    var fn = function () {
      var client, cache;
      client = new Catbox.Client(DynamoDB, internals.options);
      cache = new Catbox.Policy({
        expiresIn: 10000
      }, client, 'a\0b');
    }
    Lab.expect(fn).to.throw(Error);
    done();
  });
});

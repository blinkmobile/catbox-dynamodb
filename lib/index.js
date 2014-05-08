// Load Modules
var Hoek = require('hoek');
var AWS = require('aws-sdk');

// Declare Internals

var internals = {};

internals.defaults = {
  region: 'us-east-1',
  apiVersion: '2012-08-10',
  tableName: '',
  hashAttribute: 'segment',
  rangeAttribute: 'id'
};

exports = module.exports = internals.Connection = function (options) {
  Hoek.assert(this.constructor === internals.Connection, 'DynamoDB cache client must be instantiated using new');
  this.settings = Hoek.applyToDefaults(internals.defaults, options || {});
};

internals.Connection.prototype.start = function (callback) {
  if (!this.isReady()) {
    this.service = new AWS.DynamoDB(this.settings);
  }
  return callback();
};

internals.Connection.prototype.stop = function () {
  delete this.service;
  return;
};

internals.Connection.prototype.isReady = function () {
  return !!this.service;
};

internals.Connection.prototype.validateSegmentName = function (name) {
  if (!name) {
    return new Error('Empty string');
  }

  if (name.indexOf('\0') !== -1) {
    return new Error('Includes null character');
  }

  return null;
};

internals.Connection.prototype.get = function (key, callback) {

  if (!this.isReady()) {
    return callback(new Error('Connection not started'));
  }

  if (!key.segment) {
    return callback(null, null);
  }

  var ddbKey = {};
  ddbKey[this.settings.hashAttribute] = {S: key.segment};
  ddbKey[this.settings.rangeAttribute] = {S: key.id};

  this.service.getItem({
    TableName: this.settings.tableName,
    Key: ddbKey
  }, function (err, data) {
    var item, transformed;

    if (err) {
      return callback(err);
    }

    if (!data.Item) {
      // Item does not exist
      return callback(null, null);
    }

    try {
      item = JSON.parse(data.Item.value.S)
    } catch (err) {
      return callback(new Error('Bad value content'));
    }

    transformed = {
      item: item,
      stored: parseFloat(data.Item.stored.N),
      ttl: parseFloat(data.Item.ttl.N)
    };

    return callback(null, transformed);
  });
};

internals.Connection.prototype.set = function (key, value, ttl, callback) {

  if (!this.isReady()) {
    return callback(new Error('Connection not started'));
  }

  var record = {};
  record[this.settings.hashAttribute] = {S: key.segment};
  record[this.settings.rangeAttribute] = {S: key.id};
  record.stored = {N: Date.now().toString()};
  record.ttl = {N: ttl.toString()};

  try {
    record.value = {S: JSON.stringify(value)};
  }
  catch (err) {
    return callback(err);
  }

  this.service.putItem({
    TableName: this.settings.tableName,
    Item: record
  }, function (err) {
    if (err) {
      return callback(err);
    }
    return callback();
  });
};

internals.Connection.prototype.drop = function (key, callback) {

  if (!this.isReady()) {
    return callback(new Error('Connection not started'));
  }

  var ddbKey = {};
  ddbKey[this.settings.hashAttribute] = {S: key.segment};
  ddbKey[this.settings.rangeAttribute] = {S: key.id};

  this.service.deleteItem({
    TableName: this.settings.tableName,
    Key: ddbKey,
    ReturnValues: 'ALL_OLD'
  }, function (err, data) {
    if (err) {
      return callback(err);
    }

    if (!data.Attributes) {
      return callback(new Error('Item does not exist'));
    }

    return callback();
  });
};

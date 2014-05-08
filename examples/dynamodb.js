// Require dependencies
var Catbox = require('catbox');
var DynamoDB = require('catbox-dynamodb');

// Set up configuration objects
var options = {
  region: 'us-west-1',
  tableName: 'sampleTable'
};
var policy = {
  expiresIn: 10000
};

// Create a Catbox Client
var client = new Catbox.Client(DynamoDB, options);

// Create a Catbox Policy using the previously created Client
var cache = new Catbox.Policy(policy, client, 'segment');

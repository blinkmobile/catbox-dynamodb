Catbox-DynamoDB
===

A Catbox external caching strategy


Usage
---
    var Catbox = require('catbox');
    var DynamoDB = require('catbox-dynamodb');
    var options = {tableName: 'sampleTable'};
    var policy = {expiresIn: 10000};
    var client = new Catbox.Client(DynamoDB, options);
    var cache = new Catbox.Policy(policy, client, 'segment');


Options
---
- `region` - the DynamoDB region within AWS. Defaults to `'us-east-1'`.
- `tableName` - the table in DynamoDB you wish to use. Needs to be within the specified region.
- `hashAttribute` - the attribute specified as the Hash key in the table. Defaults to `'segment'`
- `rangeAttribute` - the attribute specified as the Range key in the table. Defaults to `'id'`

Notes
---
At this point in time it is assumed that a table has been created prior to using this caching strategy.

When the table is created it will need to have the Primary Key set to Hash and Range.

If you do not set Hash to `'segment'` and Range to `'id'`, you will need to specify these in the options for the strategy to work correctly.

Testing
---
`node test`
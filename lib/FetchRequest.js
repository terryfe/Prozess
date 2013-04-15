var BufferMaker = require('buffermaker');
var Request = require('./Request');

var FetchRequest = function(topic, partition, offset, options) {
  this.topic = topic;
  this.partition = partition;
  this.offset = offset;
  
  options = options || {};
  this.options = options || {};
  this.options.maxWaitTime = options.maxWaitTime || 100;
  this.options.minBytes = options.minBytes || 1024;
  this.options.maxBytes = options.maxBytes || 65536;  
  
};

 /*
    0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /                         REQUEST HEADER                        /
  /                                                               |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                         REPLICA_ID (-1)                       |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                          MAX_WAIT_TIME                        |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                           MIN_BYTES                           |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |       TOPICNAME_LENGTH        |             TOPIC NAME        /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
  |                              ...                              |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                            PARTITION_ID                       |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                             OFFSET                            |
  |                                                               |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                            MAX_BYTES                          |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  */
FetchRequest.prototype.toBytes = function(){  
  var body = new BufferMaker()
  .Int32BE(-1)
  .UInt32BE(this.options.maxWaitTime)
  .UInt32BE(this.options.minBytes)
  .UInt32BE(1) // topic count
  .UInt16BE(this.topic.length)
  .string(this.topic)
  .UInt32BE(1) // partition count
  .UInt32BE(this.partition)
  .Int64BE(this.offset)
  .UInt32BE(this.options.maxBytes)
  .make();
  var req = new Request(Request.Types.FETCH, 0, "xx", body);
  return req.toBytes();
};

module.exports = FetchRequest;

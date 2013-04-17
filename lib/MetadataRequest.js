var BufferMaker = require('buffermaker');
var Request = require('../lib/Request');
var _ = require('underscore');
var binary = require('binary');


var MetadataRequest = function(topic){
  this.topic = topic;
};

/*

  0               1               2               3
   
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                      TOPIC COUNT                              |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |   TOPICNAME_LENGTH            |          TOPIC NAME           /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
  /                              ...                              |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*/
MetadataRequest.prototype.toBytes = function(){
  var body = new BufferMaker()
                 .UInt32BE(1)
                 .UInt16BE(this.topic.length)
                 .string(this.topic)
                 .make();
                 
  var req = new Request(Request.Types.METADATA, 0, "", body);
  return req.toBytes();
};


module.exports = MetadataRequest;

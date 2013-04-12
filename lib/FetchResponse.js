var binary = require('binary');
var BufferMaker = require('buffermaker');
var Message = require('../lib/Message');
var MessageSet = require('../lib/MessageSet');
var Response = require('../lib/Response');
var _ = require('underscore');


/*
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                          RESPONSE HEADER                      /
  /                        (see Response.js)                      |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        TOPIC_COUNT                            |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |    TOPIC_NAME_LENGTH          |         TOPIC_NAME            /  
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
  /                              ...                              |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                         PARTITION_COUNT                       |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                         PARTITION_ID                          |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |         ERROR_CODE            |     HighwaterMarkOffset       /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
  /                                                               /
  +                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /                               |         MessageSetSize        /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
  /                               |          MessageSet           /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
  /                              ...                              |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  FetchResponse => [TopicName [Partition ErrorCode FetchedOffset HighwaterMarkOffset MessageSetSize MessageSet]]
  TopicName => string
  Partition => int32
  ErrorCode => int16
  HighwaterMarkOffset => int64
  MessageSetSize => int32

  NOTICE: For convenience only one topic with one partition is supported in one Response this version.

*/
var FetchResponse = function(head, topic, partition, error, highwaterMarkOffset, messageSet){
  this.head = head; // Common Response Header
  this.topic = topic;
  this.partition = partition;
  this.error = error;
  this.highwaterMarkOffset = highwaterMarkOffset;
  this.messageSet = messageSet;
};


FetchResponse.fromBytes = function(bytes){
  var response = Response.fromBytes(bytes);
  
  var topicNameLength = response.body.readUInt16BE(4);
  var unpacked = binary.parse(response.body)
                       .word32bu('topicCount')
                       .word16bu('topicNameLength')
                       .buffer('topic', topicNameLength);
                       .word32bu('partitionCount')
                       .word32bu('partition')
                       .word16bu('error')
                       .word64bu('highwaterMarkOffset')
                       .word32bu('messageSetSize')
                       .buffer('messageSet', 
                          response.body
                             .readUInt32BE(4 + 2 + topicNameLength
                                             + 4 + 4 + 2 + 8))
                       .vars;

  var messageSet = MessageSet.fromBytes(unpacked.messageSet);

  return new FetchResponse(response, unpacked.topic, unpacked.partition, unpacked.error, unpacked.highwaterMarkOffset, unpacked.messageSet);
};

module.exports = FetchResponse;



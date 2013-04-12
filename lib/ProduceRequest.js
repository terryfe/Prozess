var BufferMaker = require('buffermaker');
var Message = require('../lib/Message');
var Request = require('../lib/Request');
var _ = require('underscore');
var binary = require('binary');

// requireAcks: This field indicates how many acknowledgements the servers should receive before responding to the request.
//              If it is 0 the server responds immediately prior to even writing the data to disk. 
//              If it is 1 the data is written to the local machine only with no blocking on replicas. 
//              If it is -1 the server will block until the message is committed by all in sync replicas. 
//              For any number > 1 the server will block waiting for this number of acknowledgements to occur 
//                  (but the server will never wait for more acknowledgements than there are in-sync replicas).

var ProduceRequest = function(topic, partition, messageSet, requireAcks, timeout){
  this.topic = topic;
  this.partition = partition;
  this.messageSet = messageSet;

  this.requireAcks = requireAcks || 0;
  this.timeout = timeout || 0;
  this.requestType = Request.Types.PRODUCE;

  this.topicCount = 1;
  this.partitionCount = 1;
};

/*

  0               1               2               3
   
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |           REQUIRE_ACKS        |            TIMEOUT            /
  /                               |            TOPIC_COUNT        /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               /
  /                               |        TOPIC_NAME_LENGTH_1    |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /                          TOPIC_NAME_1                         /
  /                              ...                              |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        PARTITION_COUNT                        |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                            PATITION_1                         |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        MESSAGE_SET_SIZE                       |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        MESSAGE_SET_BUFFER                     /
  /                              ...                              |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                            PATITION_2                         |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        MESSAGE_SET_SIZE                       |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        MESSAGE_SET_BUFFER                     /
  /                              ...                              |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |     TOPIC_NAME_LENGTH_2       |            TOPIC_NAME_2       /
  /                              ...                              |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  Although new api in v0.8 supports batch operations, i.e. we can produce multiple topics with
  multiple patitions in one request, but for convenience here we support one topic with one partition only.
*/
ProduceRequest.prototype.toBytes = function(){
  var procuderReqHead = new BufferMaker()
                      .UInt16BE(this.requireAcks)
                      .UInt32BE(this.timeout)
                      .UInt32BE(this.topicCount)
                      .UInt16BE(this.topic.length)
                      .string(this.topic)
                      .UInt32BE(this.partitionCount)
                      .UInt32BE(this.partition)                      
                      .make();
  var messageSetBuffer = this.messageSet.toBytes();
  var body = Buffer.concat([procuderReqHead, new BufferMaker().UInt32BE(messageSetBuffer.length).make(),messageSetBuffer]);
  var req = new Request(Request.Types.PRODUCE, 0, "", body);
  return req.toBytes();
};


module.exports = ProduceRequest;

var MESSAGES_LENGTH_SIZE = 4;

var getLengthOfMessagesSegment = function(buffer) {
   return binary.parse(buffer).word32bu('length').vars.length;
};
var toMessages = function(messagesBuffer) {
  var messages = [];
  while(messagesBuffer.length > 0) {
    var message = Message.fromBytes(messagesBuffer);
    messages.push(message);
    messagesBuffer = messagesBuffer.slice(message.toBytes().length);
  }
  return messages;
};
var getMessagesSegment = function(body) {
  var start = MESSAGES_LENGTH_SIZE;
  var stop =  getLengthOfMessagesSegment(body) + MESSAGES_LENGTH_SIZE;
  return body.slice(start, stop);
};

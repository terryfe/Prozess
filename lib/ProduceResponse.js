var Response = require('../lib/Response');
var binary = require('binary');

/*
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |     TOPIC_NAME_LENGTH (N)     |          TOPIC_NAME           /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
  /                           (N length)                          |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        PARTITION_ID                           |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |         ERROR_CODE            |           Offset              /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
  |																  |
  +								  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |								  |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  CORRELATION_ID = int32 // Equal to correspond request's correlation id
  RESPONSE_BODY = All kinds of response

 */
var ProduceResponse = function(head, topic, partition, error, offset) {
	this.head = head;
	this.topic = topic;
	this.partition = partition;
	this.error = error;
	this.errorMessage = Response.ErrorMap[error.toString()];
	this.offset = offset;
}

ProduceResponse.fromBytes = function(data) {	
	var head = Response.fromBytes(data);
	var topicLength = head.body.readUInt16BE(4);
	//var topic = head.body.slice(2 + 4, topicLength);
	//var partition = head.body.readUInt32BE(2 + 4  + topicLength + 4);
	//var error = head.body.readUInt16BE(2 + 4 + topicLength + 4 + 4);
	//var offset = head.body.readUInt64BE(2 + 4 + topicLength + 4 + 4 + 2);

	var res = binary.parse(head.body)
	          .word16be('head')
	          .word32be('topicCount')
	          .buffer('topic', topicLength)
	          .word32be('partitionCount')
	          .word32be('partition')
	          .word16be('error')
	          .word64be('offset').vars;

	return new ProduceResponse(head, res.topic.toString(), res.partition, res.error, res.offset);
};

module.exports = ProduceResponse;
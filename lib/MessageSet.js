var logger = require('log4js').getLogger("MessageSet");
var BufferMaker = require('buffermaker');
var binary = require('binary');

var Message = require('../lib/Message')


function MessageSet(messages) {

	this.messages = messages || [];

}

MessageSet.prototype.toBytes = function() {	
	var i = 0;
	var bufferList = []
	this.messages.forEach(function(m){
		bufferList.push(new BufferMaker().Int64BE(i).make());
		bufferList.push(m.toBytes());
		i++;
	});

	return Buffer.concat(bufferList);		
};

MessageSet.prototype.appendMessage = function(message) {
	this.messages.push(message);
};

MessageSet.fromBytes = function(buffer) {
	var offset = 0;
	var messages = [];	

	logger.debug("Get MessageSet Length = " + buffer.length);

	while(offset < buffer.length) {

		var buf = buffer.slice(offset);
		var messageLength = buf.readUInt32BE(8) + 4;
		var unpacked = binary.parse(buf)
		                     .word64bu('offset')		                     
		                     .buffer('message', messageLength)
		                     .vars;
		offset += messageLength + 8;
		var m = Message.fromBytes(unpacked.message);
		logger.debug("Get Message ( Length = " + unpacked.message.length +" | Offset = " + unpacked.offset + " ): " + JSON.stringify(m));
		m.offset = unpacked.offset;
		messages.push(Message.fromBytes(unpacked.message));
	}

	return new MessageSet(messages);
};

module.exports = MessageSet;
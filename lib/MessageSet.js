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
	var message = null;
	var messages = [];
	var i = 0;
	while(offset < buffer.length) {

		var buf = buffer.slice(offset);
		var messageLength = buffer.readUInt32BE(8) + 4;
		var unpacked = binary.parse(buf)
		                     .word64bu('offset')		                     
		                     .buffer('message', messageLength)
		                     .vars;
		offset += messageLength + 8;
		var m = Message.fromBytes(unpacked.message);
		m.offset = unpacked.offset;
		messages.push(Message.fromBytes(unpacked.message));
	}

	return new MessageSet(messages);
};

module.exports = MessageSet;
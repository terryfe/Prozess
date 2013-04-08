var BufferMaker = require('buffermaker');


function MessageSet(messages) {

	this.messages = messages;

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

MessageSet.prototype.fromBytes = function(buffer) {
	// body...
};

module.exports = MessageSet;
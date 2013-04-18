var net = require('net');
var util = require("util");
var events = require("events");
var logger = require('log4js').getLogger('Metadata');

var Response = require('./Response');
var MetadataResponse = require('./MetadataResponse');
var MetadataRequest = require('./MetadataRequest');


var TopicAndPartition = function(topic, partition, host, port){
	this.topic = topic;
	this.partition = partition;
	this.host = host;
	this.port = port;
}

var Metadata = function(consumer) {
	this.consumer = consumer;
};


Metadata.prototype.sendMetadataRequest = function(topic) {
	var req = new MetadataRequest(topic);
    this.consumer._setRequestMode("meta");
	this.consumer.socket.write(req.toBytes());
};

Metadata.prototype.getLeaderBroker = function(topic, partition) {
	var leader = {};

	this.cache.some(function(broker){
		if(broker.topic == topic && broker.partition == partition) {
			leader.host = broker.host;
			leader.port = broker.port;
			return true;
		};
	});

	return leader;
};


Metadata.prototype.handleMetaData = function(cb) {
	logger.debug("Entering handleMetaData");
	var res;
    try {
        res = MetadataResponse.fromBytes(this.consumer.responseBuffer);        
    } catch (ex) {
        if (ex === "incomplete response") {
            // don't bother parsing.  quit out of this handler.  wait for more data.            
            return;
        } else {
            return cb(ex);
        }
    }

    if (res.topic.topicErrorCode != 0) {
    	logger.debug("Error: " + Response.ErrorMap[res.topic.topicErrorCode]);
        return cb(res.topcErrorCode);
    }

    logger.debug("Get MetadataResponse: " + JSON.stringify(res));

    var leader = res.findLeader(this.consumer.topic, this.consumer.partition);

    logger.debug("Get leader: " + JSON.stringify(leader));

    this.consumer.leader = leader;        
    this.consumer._unsetRequestMode();
    //this.consumer._reconnect(leader.host, leader.port, this.consumer.consume);
    this.consumer.options.host = leader.host;
    this.consumer.options.port = leader.port;
    this.consumer.connect(this.consumer.consume.bind(this.consumer));

    return;
};

module.exports = Metadata;
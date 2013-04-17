var net = require('net');
var util = require("util");
var events = require("events");
var log4js = require('log4js');

var MetadataResponse = require('./MetadataResponse');
var MetadataRequest = require('./MetadataRequest');


var Metadata = function(socket){
	this.cache = {}; // Store recent metadata cache
	this.socket = socket;
};


// Get topic|partition owner socket
Metadata.prototype.sendMetadataRequest = function(topic, partition, callback) {
	var req = new MetadataRequest(topic);
	this.socket.write(req.toBytes());	
};

Metadata.prototype.setSocket = function(socket) {
	this.socket = socket;
};

Metadata.prototype.handleMetadataResponse = function(first_argument) {
	// body...
};

exports.module = Metadata;
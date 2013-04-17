var net = require('net');
var util = require("util");
var events = require("events");
var log4js = require('log4js');

var _ = require('underscore');
var BufferMaker = require('buffermaker');
var bignum = require('bignum');

var Message = require('./Message');
var Metadata = require('./Metadata');
var Response = require('./Response');
var OffsetsResponse = require('./OffsetsResponse');
var FetchRequest = require('./FetchRequest');
var OffsetsRequest = require('./OffsetsRequest');
var FetchResponse = require('./FetchResponse');



var Consumer = function(options){
  this.MAX_MESSAGE_SIZE = 1024 * 1024; // 1 megabyte
  this.DEFAULT_POLLING_INTERVAL = 2; // 2 seconds
  this.MAX_OFFSETS = 1;
  this.LATEST_OFFSET = -1;
  this.EARLIEST_OFFSET = -2;

  options = options || {};
  this.options = options;
  this.topic = options.topic || 'test';
  this.partition = options.partition || 0;
  this.host = options.host || 'localhost';
  this.port = options.port || 9092;
  this.offset = bignum(options.offset);
  this.maxMessageSize = options.maxMessageSize || this.MAX_MESSAGE_SIZE;
  this.polling = options.polling  || this.DEFAULT_POLLING_INTERVAL;
  this.unprocessedData = new Buffer([]);

  this.requestMode = null;
  this.responseBuffer = new Buffer([]);
  this.offsetsResponder = function(){};
  this.fetchResponder = function(){};
  this.metadataResponder = function(){};

  this.logger = log4js.getLogger(this);

  this.metadata = new Metadata();

};

util.inherits(Consumer, events.EventEmitter);


Consumer.prototype.onOffsets = function(handler){  this.offsetsResponder = handler; };
Consumer.prototype.onFetch = function(handler){  this.fetchResponder = handler; };
Consumer.prototype.onMetadata = function(handler){  this.metadataResponder = handler; };

Consumer.prototype._setRequestMode = function(mode){
  this.requestMode = mode;
};
 
Consumer.prototype._unsetRequestMode = function(){
  this.requestMode = null;
};

Consumer.prototype.connect = function(cb){
  var that = this;
  this.socket = net.createConnection({port : this.port, host : this.host}, function(){
    cb();
  });

  this.socket.on('end', function(){
  });
  this.socket.on('timeout', function(){
  });
  this.socket.on('drain', function(){
  });
  this.socket.on('error', function(err){
    cb(err);
  });
  this.socket.on('close', function(){
  });
  this.socket.on('data', function(data){    
    that.responseBuffer = Buffer.concat([that.responseBuffer, data]); 
    if (that.MAX_MESSAGE_SIZE < that.responseBuffer.length){
      var handler = that.requestMode === 'fetch' ? that.fetchResponder : that.offsetsResponder;
      return handler(new Error("Max message was exceeded. Possible causes: bad offset, corrupt log, message larger than max message size (1048576)"));
    }
    switch(that.requestMode){
      case "fetch":
        that.handleFetchData(that.fetchResponder);
        break;
      case "offsets":
        that.handleOffsetsData(that.offsetsResponder);
        break;
      default:
        throw "Got a response when no response was expected!";
    }
  });
};

Consumer.prototype.close = function() {
  this.socket.destroy();  
};

Consumer.prototype.handleFetchData = function(cb){
  // message fetch
  var err = null;
  var response = null;
  try {
    response = FetchResponse.fromBytes(this.responseBuffer);
    this.responseBuffer = this.responseBuffer.slice(response.head.bodyLength + 8);
  } catch(ex){
    if (ex === "incomplete response" || ex === "incomplete message"){
      console.log("incomplete message, wait for next...")
      // don't bother parsing.  quit out of this handler.  wait for more data.
      return;
    } else {
      return cb(ex);
    }
  }

  
  // Metadata is out of date. Reissue metadata request.
  if(response.error == Response.Errors.WrongTopicOrPartition) {
    this._setRequestMode("meta");

  }

  if(response.error != Response.Errors.NoError) {
    return cb(new Error(Response.ErrorMap[response.error]), null);
  }

  var messages = response.messages;
  var messageLength = 0;
  _.each(messages, function(message){
    messageLength += message.toBytes().length;
  });
  this.incrementOffset(messageLength);
  this._unsetRequestMode();
  return cb(null, response);
  
};

Consumer.prototype.handleOffsetsData = function(cb){
  var res;
  try {
    res = OffsetsResponse.fromBytes(this.responseBuffer);
  } catch(ex){
    if (ex === "incomplete response"){
      // don't bother parsing.  quit out of this handler.  wait for more data.
      return;
    } else {
      return cb(ex);
    }
  }
  if (!!res.error) {
    return cb(res.error);
  }
  this._unsetRequestMode();
  this.responseBuffer = this.responseBuffer.slice(res.byteLength());
  var offsets = res.offsets;
  return cb(null, res);
};

Consumer.prototype.sendConsumeRequest = function(cb){  
  if (this.offset === null || this.offset === undefined || !this.offset.eq) {
    return cb("offset was " + this.offset);
  }
  this._setRequestMode("fetch");
  this.onFetch(cb);
  var that = this;  
  var fetchRequest =  new FetchRequest(this.topic, this.partition, this.offset, this.options);
  var request = fetchRequest.toBytes();
  this.socket.write( request );
};

Consumer.prototype.consume = function(cb){
  var cb = function(){};
  var consumer = this;
  var offset;
  if (this.offset === null){
      this.getOffsets(function(err, offsetsResponse){
        if (err){
          return cb(err);
        }
        var offsets = offsetsResponse.offsets;
        offset = offsets[0];
        consumer.setOffset(offset);
        consumer.sendConsumeRequest(function(err, fetchResponse){
          handleFetchResponse(consumer, err, fetchResponse, cb);
        });
      });
  } else {
    consumer.sendConsumeRequest(function(err, fetchResponse){
      handleFetchResponse(consumer, err, fetchResponse, cb);
    });
  }

};

Consumer.prototype.setOffset = function(offset){
  this.offset = bignum(offset);
};
Consumer.prototype.incrementOffset = function(value){
  this.setOffset(this.offset.add(bignum(value)));
};


var handleFetchResponse = function(consumer, err, response, cb){
  if (!!err){
  }
  if (err){
    consumer.emit('error', err)
    return;
  }
  
  consumer.emit('message', response.messages);
};

Consumer.prototype.getOffsets = function(cb){
  this.onOffsets(cb);
  this._setRequestMode("offsets");
  var that = this;
  var request = new OffsetsRequest(this.topic, this.partition, -1, this.MAX_OFFSETS);
  this.socket.write(request.toBytes());
 
};


module.exports = Consumer;


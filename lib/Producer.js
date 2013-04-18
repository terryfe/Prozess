var net = require('net');
var EventEmitter = require('events').EventEmitter;
var logger = require('log4js').getLogger("Producer");
var dns = require('dns');

var _ = require('underscore');

var Message = require('./Message');
var MessageSet = require('./MessageSet');
var Response = require('./Response');
var ProduceRequest = require('./ProduceRequest');
var ProduceResponse = require('./ProduceResponse');
var Metadata = require('./Metadata');
var MetadataRequest = require('./MetadataRequest');
var MetadataResponse = require('./MetadataResponse');



var Producer = function(topic, options){
  if (!topic || (!_.isString(topic))){
    throw "the first parameter, topic, is mandatory.";
  }
  this.MAX_MESSAGE_SIZE = 1024 * 1024; // 1 megabyte
  this.options = options || {};
  this.topic = topic;
  this.partition = options.partition || 0;
  this.host = options.host || 'localhost';
  this.port = options.port || 9092;

  this.connection = null;
  this.connecting = false;

  this.messagesQueue = [];
};

Producer.prototype = Object.create(EventEmitter.prototype);

Producer.prototype.connect = function(){
  var that = this;
  this.mode = "produce";
  this.connecting = true;

  dns.lookup(this.host, function(error, address){
    if(error) {
      that.emit('error', error);
    }
    logger.debug("Connecting to " + address);
    that.connection = net.createConnection(that.port, address);
    that.connection.setKeepAlive(true, 1000);
    that.connection.once('connect', function(){
      that.connecting = false;
      if(that.messagesQueue.length > 0) {
        that.send(that.messagesQueue);
      }
      that.emit('connect');
    });
    that.connection.once('error', function(err){
      if (!!err.message && err.message === 'connect ECONNREFUSED'){
        that.emit('error', err);
        that.connecting = false;
      }
    });
  });
};

Producer.prototype._reconnect = function(cb){
  var producer = this;

  var onConnect = function(){
    producer.removeListener('brokerReconnectError', onBrokerReconnectError);
    return cb();
  };
  producer.once('connect', onConnect);

  var onBrokerReconnectError = function(err){
    producer.removeListener('connect', onConnect);
    return cb('brokerReconnectError');
  };
  producer.once('brokerReconnectError', onBrokerReconnectError);

  if (!producer.connecting){
    producer.connect();
    producer.connection.on('error', function(err){
      if (!!err.message && err.message === 'connect ECONNREFUSED'){
        producer.emit("brokerReconnectError", err);
      } else {
        // don't care about other errors that may fire here
      }
    });
  } else {
    // reconnect already in progress.  wait.
  }
};


Producer.prototype.sendMetadataRequest = function() {
  var req = new MetadataRequest(this.topic);
  this.mode = "meta";
  this.connection.write(req.toBytes());
};

Producer.prototype.send = function(messages) {
  var that = this;
  this.messagesQueue.concat(messages);

  messages = new MessageSet(toListOfMessages(toArray(messages)));
  var request = new ProduceRequest(this.topic, this.partition, messages, this.options.requireAcks, this.options.timeout);
  this.connection.on('data', function(data){
    if(that.mode == "produce") {
      var res = ProduceResponse.fromBytes(data);
      if(res.error == Response.Errors.NoError) {
        that.messagesQueue = [];
        if(that.options.requireAcks) {
          that.emit('sent', null, res);  
        } else{
          that.emit('sent');
        }        
        return;
      };

      if(res.error == Response.Errors.WrongTopicOrPartition ) {
        // Not leader, send metadata request
        logger.debug("Not leader...");
        that.sendMetadataRequest();
        return;
      }

      
      that.emit('error', new Error(Response.ErrorMap[res.error]), res);  
    }    

    if(that.mode == "meta") {
      var res = MetadataResponse.fromBytes(data);
      
      var leader = res.findLeader(that.topic, that.partition);
      that.host = leader.host;
      that.port = leader.port;
      
      that.connection.end();
      that.connect();
    }

  });
  this.connection.write(request.toBytes(), function(err) {
    if (!!err && err.message === 'This socket is closed.') {
      that._reconnect(function(err){
        if (err){
          return that.emit('error', err);
        }
        that.connection.write(request.toBytes(), function(err) {
          if(err) {
            // TODO: Log error here.
            return that.emit(err, null);  
          }
        });
      });
    } else {
      if(err) {
        // TODO: Log error here.
        return that.emit(err, null);  
      }
    }
  });
};

module.exports = Producer;

var toListOfMessages = function(args) {
  return _.map(args, function(arg) {
    if (arg instanceof Message) {
      return arg;
    }
    return new Message(arg);
  });
};

var toArray = function(arg) {
  if (_.isArray(arg))
    return arg;
  return [arg];
};

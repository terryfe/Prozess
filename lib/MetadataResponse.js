var binary = require('binary');
var BufferMaker = require('buffermaker');
var Response = require('../lib/Response');
var _ = require('underscore');
var logger = require('log4js').getLogger("MetadataResponse");



var MetadataResponse = function(head, brokers, topic){
  this.head = head;
  this.brokers = brokers;
  this.topic = topic;
};


/*
  MetadataResponse => [Broker][TopicMetadata]
  Broker => NodeId Host Port
  NodeId => int32
  Host => string
  Port => int32
  TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
  PartitionErrorCode => int16
  PartitionId => int32
  Leader => int32
  Replicas => [int32]
  Isr => [int32]

*/

MetadataResponse.fromBytes = function(bytes){
  var response = Response.fromBytes(bytes);

  var brokers = [];
  var brokerCount = response.body.readUInt32BE(0);

  logger.debug("Parsing MetadataResponse...");
  logger.debug("  Broker Count: " + brokerCount);

  var offset = 4; // Start from broker count field.

  for(var i = 0; i < brokerCount; i++) {    
    var broker =  binary.parse(response.body.slice(offset))
                          .word32bu('nodeId')
                          .word16bu('hostLength')
                          .vars;
        broker =  binary.parse(response.body.slice(offset))
                          .word32bu('nodeId')
                          .word16bu('hostLength')
                          .buffer('host', broker.hostLength)
                          .word32bu('port')
                          .vars;

    brokers.push(broker);
    offset += 4 + 2 + broker.hostLength + 4;
  }

  logger.debug("  Brokers: " + JSON.stringify(brokers));

  offset += 4; // Only one topic in a request is supported, skip topic metadata count field.

  var metadata = binary.parse(response.body.slice(offset))
                       .word16bu('topicErrorCode')
                       .word16bu('topicLength')
                       .vars;

      metadata = binary.parse(response.body.slice(offset))
                       .word16bu('topicErrorCode')
                       .word16bu('topicLength')
                       .buffer('topicName', metadata.topicLength)
                       .word32bu('partitionCount')
                       .vars;

  offset += 2 + 2 + metadata.topicLength + 4; // skip to partition metadata

  metadata.partitions = [];

  for(var i = 0; i < metadata.partitionCount; i++) {
    var partition = binary.parse(response.body.slice(offset))
                          .word16bu('partitionErrorCode')
                          .word32bu('partitionId')
                          .word32bu('leader')
                          .word32bu('replicaCount')
                          .vars;

    offset += 2 + 4 + 4 + 4; // skip to replicas

    var replicas = []; 
    for(var j = 0; j < partition.replicaCount; j ++) {
      replicas.push(response.body.readUInt32BE(offset));
      offset += 4;
    }    

    partition.replicas = replicas;

    var isrCount = response.body.readUInt32BE(offset);

    offset += 4 + 4 * isrCount; // Ignore Isr...

    metadata.partitions.push(partition);
  }
  
  logger.debug("  Metadata: " + JSON.stringify(metadata));

  return new MetadataResponse(response, brokers, metadata);
};

MetadataResponse.prototype.findLeader = function(topic, partition) {  
  var leaderId;
  var leader = null;

//  if(this.metadata.topicName != topic) {
//    return false;
//  }

  logger.debug("Searching partition leader ID for partition " + partition);

  this.topic.partitions.some(function(p){
    if(p.partitionId == partition) {
      leaderId = p.leader;
      return true;
    }
  });

  logger.debug("Search broker for leader ID " + leaderId);

  this.brokers.some(function(b){
    if(b.nodeId == leaderId) {
      leader = b;
      return true;
    }
  });
  
  return leader;
};

module.exports = MetadataResponse;



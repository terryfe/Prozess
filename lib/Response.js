var binary = require('binary');
var BufferMaker = require('buffermaker');
var Message = require('../lib/Message');
var ProduceResponse = require('../lib/ProduceResponse');
var _ = require('underscore');





/*
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1

  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                         LENGTH                                | 
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        CORRELATION_ID                         |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                         RESPONSE_BODY                         |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  CORRELATION_ID = int32 // Equal to correspond request's correlation id
  RESPONSE_BODY = All kinds of response

 */
var Response = function(cid, bodyLength, body){    
  if (!Buffer.isBuffer(body)){
    throw "The body parameter must be a Buffer object.";
  }
  this.body = body;  // a Buffer object
  this.bodyLength = bodyLength;
  this.cid = cid;
};

Response.Types = {
    PRODUCE      : 0,
    FETCH        : 1,
    OFFSETS      : 2,
    META         : 3,
    LEADANDLSR   : 4,
    STOPREPLICA  : 5,
    OFFSETCOMMIT : 6,
    OFFSETFETCH  : 7
};

Response.Errors = {
  'Unknown'         : -1,           // Unknown Error
  'NoError' :          0,           // Success
  'OffsetOutOfRange' : 1,           // Offset requested is no longer available on the server
  'InvalidMessage' :   2,           // A message you sent failed its checksum and is corrupt.
  'WrongTopicOrPartition' : 3,      // You tried to access a partition that doesn't exist
                                    //    (was not between 0 and (num_partitions - 1)).
  'InvalidMessageSize': 4,          // The size you requested for fetching is smaller than
                                    //    the message you're trying to fetch.
  'LeaderNotAvailable': 5,          //  This error is thrown if we are in the middle of a leadership election and 
                                    //    there is currently no leader for this partition and hence it is unavailable for writes.
  'NotLeaderForPartition': 6,       // This error is thrown if the client attempts to send messages to a replica that is not the 
                                    //    leader for some partition. It indicates that the clients metadata is out of date.
  'RequestTimedOut': 7,             // This error is thrown if the request exceeds the user-specified time limit in the request.
  'BrokerNotAvailable': 8,          // This is not a client facing error and is used only internally by intra-cluster broker communication.  
  'ReplicaNotAvailable': 9,         // What is the difference between this and LeaderNotAvailable?
  'MessageSizeTooLarge': 10,        // The server has a configurable maximum message size to avoid unbounded memory allocation. 
                                    //    This error is thrown if the client attempt to produce a message larger than this maximum.                          
  'StaleControllerEpochCode': 11,   // ???
  'OffsetMetadataTooLargeCode': 12  // If you specify a string larger than configured maximum for offset metadata
};

Response.ErrorMap = {
  "-1": 'Unknown', 
  0:  'NoError' ,
  1:  'OffsetOutOfRange',
  2:  'InvalidMessage',
  3:  'WrongTopicOrPartition',                       
  4:  'InvalidMessageSize',                       
  5:  'LeaderNotAvailable',                                    
  6:  'NotLeaderForPartition',                        
  7:  'RequestTimedOut',
  8:  'BrokerNotAvailable',
  9:  'ReplicaNotAvailable',
  10: 'MessageSizeTooLarge',                                    
  11: 'StaleControllerEpochCode',
  12: 'OffsetMetadataTooLargeCode'
};

Response.fromBytes = function(data){
  if (!Buffer.isBuffer(data)){
    throw "The data parameter must be a Buffer object.";
  }

  var bodyLength = data.readUInt32BE(0);
  var cid = data.readUInt32BE(4);
  var body = data.slice(8);
  
  if(body.length + 4 < bodyLength) {    
    throw "incomplete response";
  }

  return new Response(cid, bodyLength, body);
};

module.exports = Response;

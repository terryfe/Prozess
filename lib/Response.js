var binary = require('binary');
var BufferMaker = require('buffermaker');
var Message = require('../lib/Message');
var _ = require('underscore');

/*
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        RESPONSE_LENGTH                        |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |         ERROR_CODE            |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  RESPONSE_LENGTH = int32 // Length in bytes of entire response (excluding this field)
  ERROR_CODE = int16 // See table below.

  ================  =====  ===================================================
  ERROR_CODE        VALUE  DEFINITION
  ================  =====  ===================================================
  Unknown            -1    Unknown Error
  NoError             0    Success
  OffsetOutOfRange    1    Offset requested is no longer available on the server
  InvalidMessage      2    A message you sent failed its checksum and is corrupt.
  WrongPartition      3    You tried to access a partition that doesn't exist
                           (was not between 0 and (num_partitions - 1)).
  InvalidFetchSize    4    The size you requested for fetching is smaller than
                           the message you're trying to fetch.
  ================  =====  ===================================================

 */
var Response = function(error, body){
  if (error < -1 || error > 4){ 
    throw "Invalid error code: " + error;
  }
  this.error = error;
  if (!Buffer.isBuffer(body)){
    throw "The body parameter must be a Buffer object.";
  }
  this.body = body;  // a Buffer object
  this.length = body.length + 2;  // body length + error field length
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

Response.fromBytes = function(data){
  if (!Buffer.isBuffer(data)){
    throw "The data parameter must be a Buffer object.";
  }
  var length = binary.parse(data).word32bu('length').vars.length;
  var unpacked = binary.parse(data)
      .word32bu('length')
      .word16bs('error')
      .buffer('body', length)
      .vars;
      if (unpacked.body.length + 4 < length){
        throw "incomplete response";
      }
  return new Response(unpacked.error, unpacked.body);
};

Response.prototype.toBytes = function(){
  return new BufferMaker()
               .UInt32BE(this.body.length + 2)  // + 2 bytes for the errorCode
               .UInt16BE(this.error)
               .string(this.body)
               .make();
};

module.exports = Response;

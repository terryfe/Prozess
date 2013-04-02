var BufferMaker = require('buffermaker');
var binary = require('binary');

var Request = function(type, correlationId, clientId, body){
  if (type < 0 || type > 7){ 
    throw "Invalid request type: " + type;
  }
  if (!Buffer.isBuffer(body)){
    throw "The body parameter must be a Buffer object.";
  }

  this.versionId = 0;
  this.type = type;
  this.correlationId = correlationId;
  this.clientId = clientId;  
  this.body = body;
};

Request.Types = {
    PRODUCE      : 0,
    FETCH        : 1,
    OFFSETS      : 2,
    META         : 3,
    LEADANDLSR   : 4,
    STOPREPLICA  : 5,
    OFFSETCOMMIT : 6,
    OFFSETFETCH  : 7
};

/*
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                       REQUEST_LENGTH                          |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |         API_KEY               |           API_VERSION         |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                          CORRELATION_ID                       |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |        CLIENT_ID LENGTH       |                               /
  /                  CLIENT_ID (variable length)                  |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                           REQUEST_BODY                        |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  REQUEST_LENGTH = int32 // Length in bytes of entire request (excluding this field)
  API_KEY        = int16 // See enum above
  API_VERSION    = int16 // Version ID, current 0

  CORRELATION_ID = int32 // User specify
                 
  REQUEST_BODY   = Variable request depends on API_KEY
*/
Request.prototype.toBytes = function(){
  return Buffer.concat([ new BufferMaker()
        .UInt32BE(2 + 2 + 4 + 2 + this.clientId.length + this.body.length) 
        .UInt16BE(this.type)
        .UInt16BE(this.versionId)
        .UInt32BE(this.correlationId)
        .UInt16BE(this.clientId.length)
        .string(this.clientId)        
        .make(), this.body]);
};

Request.fromBytes = function(bytes) {
  var vars = binary.parse(bytes)
                     .word32bu('length')
                     .word16bu('type')
                     .word16bu('topicLength')
                     .tap( function(vars) {
                       this.buffer('topic', vars.topicLength);
                     })
                     .word32bu('partition')
                     .tap( function(vars) {
                       this.buffer('body', vars.length - 2 - 2 - 4 - vars.topicLength);
                     })
                     .vars;
  return new Request(vars.topic.toString(), vars.partition, vars.type, vars.body);
};

module.exports = Request;

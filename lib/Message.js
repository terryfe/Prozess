var crc32 = require('crc32');
var binary = require('binary');
var BufferMaker = require('buffermaker');
var _ = require('underscore');
var BASIC_MESSAGE_HEADER_SIZE = 5;
var MESSAGE_SIZE_BYTE_LENGTH = 4;
var V0_7_MESSAGE_HEADER_SIZE = 10;
var V0_6_MESSAGE_HEADER_SIZE = 9;
var COMPRESSION_DEFAULT = 0;
/*
  A message. The format of an N byte message is the following:
  1 byte "magic" identifier to allow format changes
  4 byte CRC32 of the payload
  N - 5 byte payload
*/

function Message(payload, checksum, compression, key){
  // note: payload should be a Buffer
  var _compression = COMPRESSION_DEFAULT;

  this.magic = 0; // in v0.8 always 0

  this.compression = compression || _compression;
    
  if (!payload && payload !== ''){
    throw "payload is a required argument";
  }
  this.payload = payload;
  this.checksum = checksum || this.calculateChecksum();

  this.key = key || "";
}

Message.prototype.byteLength = function(){
  if (!this.bytesLengthVal){
    this.toBytes();
  }
  return this.bytesLengthVal;
};

var calculateChecksum = function(p){
  return parseInt(crc32(p), 16);
};


Message.prototype.calculateChecksum = function(p){
  return parseInt(crc32(p), 16);
};

Message.prototype.toBytes = function(){
  var nonLength = new BufferMaker()
    .UInt8(this.magic)
    .UInt8(this.compression)
    .UInt32BE(this.key.length)
    .string(this.key)
    .UInt32BE(this.payload.length)
    .string(this.payload)
    .make();

  var withCRC = new BufferMaker()
    .UInt32BE(calculateChecksum(nonLength.toString('binary')))
    .string(nonLength)
    .make();

  var encodedMessage = new BufferMaker()
    .UInt32BE(withCRC.length)
    .string(withCRC)
    .make();

  this.bytesLengthVal = encodedMessage.length;
  return encodedMessage;

};


/*

  // 0                 1               2               3
  //                              New Format
  // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  // |                            LENGTH (N)                         |
  // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  // |                             CRC32                             |
  // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  // |     MAGIC (2)   |  COMPRESSION  |           KEY LENGTH (K)    |
  // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  // |      KEY LENGTH (cont.)         |           KEY               /
  // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                             /
  // /                         KEY  (cont.)                          /
  //                              ...                                |
  // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  // /                         PAYLOAD  (N - K - 10)                 /
  // /                                ...                            |
  // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*/

Message.fromBytes = function(buf) {

  var keyLength = buf.readUInt32BE(10);
  if(keyLength == 0xffffffff) {
    keyLength = 0;
  }
  var unpacked = binary.parse(buf)
            .word32bu('length')
            .word32bu('crc32')
            .word8u('magic')
            .word8u('compression')
            .word32bu('keyLength')
            .buffer('key', keyLength)
            .word32bu('payloadLength')
            .vars;

  if (unpacked.length + 4 > buf.length || unpacked.key.length < keyLength) {    
    throw "incomplete message";
  }  


    
  var payloadLength = unpacked.payloadLength;

  var unpacked = binary.parse(buf)
            .word32bu('length')
            .word32bu('crc32')
            .word8u('magic')
            .word8u('compression')
            .word32bu('keyLength')
            .buffer('key', keyLength)
            .word32bu('payloadLength')
            .buffer('payload', payloadLength)
            .vars;  

  if (unpacked.payload.length < payloadLength){
    throw "incomplete message";
  }

  if(unpacked.crc32 != calculateChecksum(buf.slice(8))) {
    throw "invalid checksum";
  }

  return new Message(unpacked.payload, unpacked.crc32, unpacked.compression, unpacked.key);
};

module.exports = Message;


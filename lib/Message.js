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

  this.magic = 0; // in v0.8 always 2

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

Message.prototype.calculateChecksum = function(p){
  console.log("CRC32 = " + parseInt(crc32(p),16));
  return parseInt(crc32(p), 16);
};

Message.prototype.isValid = function(){
  return this.checksum == this.calculateChecksum();
};


/*
 
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                             LENGTH                            |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |     MAGIC       |  COMPRESSION  |           CHECKSUM          |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |      CHECKSUM (cont.)           |           PAYLOAD           /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                             /
  /                         PAYLOAD (cont.)                       /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  LENGTH = int32 // Length in bytes of entire message (excluding this field)
  MAGIC = int8 // 0 = COMPRESSION attribute byte does not exist (v0.6 and below)
               // 1 = COMPRESSION attribute byte exists (v0.7 and above)
  COMPRESSION = int8 // 0 = none; 1 = gzip; 2 = snappy;
                     // Only exists at all if MAGIC == 1
  CHECKSUM = int32  // CRC32 checksum of the PAYLOAD
  PAYLOAD = Bytes[] // Message content

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
    .UInt32BE(this.calculateChecksum(nonLength.toString('binary')))
    .string(nonLength)
    .make();

  var encodedMessage = new BufferMaker()
    .UInt32BE(withCRC.length)
    .string(withCRC)
    .make();

  this.bytesLengthVal = encodedMessage.length;
  return encodedMessage;

};


// Not implemented for v0.8
Message.fromBytes = function(buf){
  // Format is:
  //  32-bit unsigned Integer, network (big-endian) byte order
  //  8-bit unsigned Integer
  //  32-bit unsigned Integer, network (big-endian) byte order

  this.bytesLengthVal = buf.length;

  var unpacked = binary.parse(buf)
            .word32bu('size')
            .word8u('magic')
            .vars;

  var size = unpacked.size;
  var payloadSize;

  if (unpacked.magic === 0){
      payloadSize = size - 5;
      unpacked = binary.parse(buf)
        .word32bu('size')
        .word8u('magic')
        .word32bu('checksum')
        .buffer('payload', payloadSize)
        .vars;
  } else {
    // magic is assumed to be 1 here.
    // if it's not, it's invalid and will throw an error when we construct later
      payloadSize = size - 6;
      unpacked = binary.parse(buf)
        .word32bu('size')
        .word8u('magic')
        .word8u('compression')
        .word32bu('checksum')
        .buffer('payload', payloadSize)
        .vars;
  }

  var magic = unpacked.magic;
  var compression = unpacked.compression;
  var checksum = unpacked.checksum;
  var payload  = unpacked.payload;

  if (payload.length < payloadSize){
    throw "incomplete message";
  }

  return new Message(payload, checksum, magic, compression);
};

module.exports = Message;


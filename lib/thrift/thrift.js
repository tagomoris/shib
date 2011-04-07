var sys = require('sys');

var Type = exports.Type = {
  STOP: 0,
  VOID: 1,
  BOOL: 2,
  BYTE: 3,
  I08: 3,
  DOUBLE: 4,
  I16: 6,
  I32: 8,
  I64: 10,
  STRING: 11,
  UTF7: 11,
  STRUCT: 12,
  MAP: 13,
  SET: 14,
  LIST: 15,
  UTF8: 16,
  UTF16: 17,
}

exports.MessageType = {
  CALL: 1,
  REPLY: 2,
  EXCEPTION: 3,
  ONEWAY: 4,
}

var TException = exports.TException = function(message) {
  Error.call(this, message);
  this.name = 'TException';
}
sys.inherits(TException, Error);

var TApplicationExceptionType = exports.TApplicationExceptionType = {
  UNKNOWN: 0,
  UNKNOWN_METHOD: 1,
  INVALID_MESSAGE_TYPE: 2,
  WRONG_METHOD_NAME: 3,
  BAD_SEQUENCE_ID: 4,
  MISSING_RESULT: 5
}

var TApplicationException = exports.TApplicationException = function(type, message) {
  TException.call(this, message);
  this.type = type || TApplicationExceptionType.UNKNOWN;
  this.name = 'TApplicationException';
}
sys.inherits(TApplicationException, TException);

TApplicationException.prototype.read = function(input) {
  var ftype
  var fid
  var ret = input.readStructBegin('TApplicationException')

  while(1){

      ret = input.readFieldBegin()

      if(ret.ftype == Type.STOP)
          break

      var fid = ret.fid

      switch(fid){
          case 1:
              if( ret.ftype == Type.STRING ){
                  ret = input.readString()
                  this.message = ret
              } else {
                  ret = input.skip(ret.ftype)
              }

              break
          case 2:
              if( ret.ftype == Type.I32 ){
                  ret = input.readI32()
                  this.type = ret
              } else {
                  ret   = input.skip(ret.ftype)
              }
              break

          default:
              ret = input.skip(ret.ftype)
              break
      }
      input.readFieldEnd()
  }

  input.readStructEnd()
}

TApplicationException.prototype.write = function(output){
  output.writeStructBegin('TApplicationException');

  if (this.message) {
      output.writeFieldBegin('message', Type.STRING, 1)
      output.writeString(this.message)
      output.writeFieldEnd()
  }

  if (this.code) {
      output.writeFieldBegin('type', Type.I32, 2)
      output.writeI32(this.code)
      output.writeFieldEnd()
  }

  output.writeFieldStop()
  output.writeStructEnd()
}

exports.objectLength = function(obj) {
  return Object.keys(obj).length;
}

exports.inherits = function(constructor, superConstructor) {
  sys.inherits(constructor, superConstructor);
}

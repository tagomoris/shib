var sys = require('sys'),
    net = require('net');

var ttransport = require('./transport');
var BinaryParser = require('./binary_parser').BinaryParser,
    TBinaryProtocol = require('./protocol').TBinaryProtocol;

exports.createServer = function(cls, handler, options) {
  if (cls.Processor) {
    cls = cls.Processor;
  }
  var processor = new cls(handler);
  var transport = (options && options.transport) ? options.transport : ttransport.TFramedTransport;
  var protocol = (options && options.protocol) ? options.protocol : TBinaryProtocol;

  return net.createServer(function(stream) {
    stream.on('data', transport.receiver(function(transport_with_data) {
      var input = new protocol(transport_with_data);
      var output = new protocol(new transport(undefined, function(buf) {
        stream.write(buf);
      }));

      try {
        processor.process(input, output);
        transport_with_data.commitPosition();
      }
      catch (e) {
        if (e instanceof ttransport.InputBufferUnderrunError) {
          transport_with_data.rollbackPosition();
        }
        else {
          throw e;
        }
      }
    }));

    stream.on('end', function() {
      stream.end();
    });
  });
};

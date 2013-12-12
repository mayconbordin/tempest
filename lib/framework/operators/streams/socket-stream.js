var net          = require('net')
  , BaseOperator = require('../base.js')
  , Tuple        = require('../../tuple.js')
  ;


/**
 * @class SocketStream
 * @classdesc Fetches continuous data from a socket and send it to downstream
 *            subscribers.
 *
 * @param {object} opts The options of the `net.connect` function
 **/
var SocketStream = function(opts) {
    this.opts   = opts;
    this.buffer = '';
    this.inputSize = 0;
};

SocketStream.prototype = new BaseOperator;

SocketStream.prototype.init = function(opts) {
    var self = this;
    
    // call parent constructor
    BaseOperator.prototype.init.call(this, opts);

    this.client = net.connect(this.opts);
    
    this.client.on('data', function(data) {
        self.buffer += data.toString();
        self.inputSize += data.length;
        
        if (self.buffer.indexOf('\n') != -1) {
            var lines = self.buffer.split('\n');
            
            self.buffer = lines.pop();
            
            lines.forEach(function(line) {
                if (line.length < 2) return;
                
                var tuple = new Tuple({
                    streamId: self.id,
                    id: self.tupleInCount++,
                    timestamp: (new Date).getTime(),
                    values: [line]
                });
                
                var prefix = (self.partitioner) ? self.partitioner.do(tuple) : '';

                self.outSocket.send(prefix + ' ' + JSON.stringify(tuple));
                self.tupleOutCount++;
            });
        }
    });
    
    
    this.client.on('error', function(e) {
        self.error('Unable to establish connection: ' + e.message);
    });
};

SocketStream.prototype.reportStatus = function() {
    var status = {
        type: 'status',
        in: this.tupleInCount,
        out: this.tupleOutCount,
        sz: this.inputSize,
        ts: (new Date).getTime(),
        op: this.id
    };

    process.send(JSON.stringify(status));
};

module.exports = SocketStream;

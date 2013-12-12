var net          = require('net')
  , BaseOperator = require('../base.js');

/**
 * @class SocketSink
 * @classdesc Receives tuples and send them to a socket
 *
 * @param {number} port The port number where the socket will be created
 **/
var SocketSink = function(port) {
    this.port   = port;
    this.server = null;
    this.socket = null;
};

SocketSink.prototype = new BaseOperator;

SocketSink.prototype.init = function(opts) {
    var self = this;
    
    BaseOperator.prototype.init.call(this, opts);
    
    this.server = net.createServer(function(c) {
        c.setMaxListeners(0);
        self.socket = c;
        
        c.on('end', function() {
            self.socket = null;
        });
    });
    
    this.server.listen(this.port, function() {
        self.logger.info("SocketSink is bound");
    });
    
    this.logger.info("SocketSink started at " + this.port);
};

SocketSink.prototype.execute = function(tuple) {
    if (this.socket) {
        this.socket.write(JSON.stringify(tuple));
        this.socket.pipe(this.socket);
        this.tupleOutCount++;
    }
};

module.exports = SocketSink;

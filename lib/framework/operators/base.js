var zmq          = require('zmq')
  , log4js       = require('log4js')
  , path         = require('path')
  , cbuffer      = require('../../../vendor/cbuffer.js')
  , Tuple        = require('../tuple.js')
  , partitioners = require('../partitioners')
  ;

/**
 * @class BaseOperator
 * @classdesc Base class for building operators, streams and sinks
 *
 * @param {function} [fn=] The function that will process each tuple/window
 **/
var BaseOperator = function(fn) {
    if (fn) this.execute = fn;
    
    this.id = null;

    
    this.buffer         = null;
    this.tupleInCount   = 0;
    this.tupleOutCount  = 0;
    
    // sliding window
    this.window         = false;
    this.windowSize     = 1;
    // advance = 0: remove the earliest tuple from the window
    // 0 < advance <= size: remove the advance earliest tuples from the window
    // advance = size: a tumbling window, all tuples are removed
    this.windowAdvance  = 1;
    this.windowType     = null;
    
    // partitioner
    this.partitioner      = null;
    this.partitionerClass = null;
    this.partitionerOpts  = {};
    
    // the id of the node to place the operator
    this.nodePlacement  = null;
    
    this.inputs         = [];
    
    this.outSocket      = null;
    this.inSockets      = [];
    
    this.logger         = null;
    
    this.interval       = null;
    this.reportInterval = 3000;
};

/**
 * Initializes the operator for sending/receiving tuples
 * @param {object} opts The operator configuration
 * @return {null}
 **/
BaseOperator.prototype.init = function(opts) {
    var self = this;

    try {
        this.logger = log4js.getLogger(this.id);
        
        if (opts.output) {
            this.outSocket = zmq.socket('pub');
            this.outSocket.identity = 'pub:' + this.id;
            this.outSocket.bindSync(opts.output);
            
            // you have to set the partitioner
            // he will receive a list of subscribers
            // check if opts.partitioner exists
            // if it doesn't then there is no partitioning of data
            // else you get the partitioner from a registry
            
            if (this.partitionerClass) {
                this.partitioner = new this.partitionerClass(opts.operator.subscribers,
                                                             this.partitionerOpts);
            }
            
            // if there is only a single subscriber it does not make
            // sense to have a partitioner
            
            this.logger.info('Output at ' + opts.output);
        }
        
        if (opts.input) {
            opts.input.forEach(function(input, k) {
                var socket = zmq.socket('sub');
                socket.identity = 'sub' + k + ':' + self.id;
                socket.connect(input.addr);
                
                // subscribe only to what is yours
                // if there is a partitioner
                if (input.partitioner)
                    socket.subscribe(self.id);
                else
                    socket.subscribe('');
                
                socket.on('message', function(data) {
                    // need to remove the prefix
                    // if there is a partitioner
                    if (input.partitioner) {
                        var msg = data.toString();
                        data = msg.substr(msg.indexOf(' ') + 1);
                    }
                
                    if (opts.rawData) self.receive(data);
                    else self.receive(JSON.parse(data));
                });
                
                self.inSockets.push(socket);
                self.logger.info('Input from ' + input.addr);
            });
        }
        
        var msg = {type: 'init', op: this.id};
        process.send(JSON.stringify(msg));
        
        // start reporting status
        this.inverval = setInterval(function() {
            self.reportStatus();
        }, this.reportInterval);
        
    } catch(e) {
        this.error('Error on initialization of operator ' + this.id + ': ' + e.message);
        console.error(e.stack);
    }
};

/**
 * Receives a tuple/window, do something with it and send new tuples
 * @abstract
 **/
BaseOperator.prototype.execute = function(tuple) {
    throw new Error('BaseOperator.execute is not implemented!');
};

BaseOperator.prototype.receive = function(tuple) {
    this.tupleInCount++;
    
    if (this.window) {
        this.buffer.push(tuple);

        if (this.windowType == BaseOperator.windowTypes.TUPLE) {
            
            if (this.tupleInCount%this.windowSize == 0) {
                this.execute(this.buffer.toArray());
                
                // TODO: test this please!
                if (this.windowAdvance == this.windowSize)
                    this.buffer.empty();
                else if (this.windowAdvance == 0)
                    this.buffer.shift();
                else if (this.windowAdvance > 0 && this.windowAdvance < this.windowSize)
                    this.buffer.remove(this.windowAdvance);
            }
        } else {

        }
    } else {
        this.execute(tuple);
    }
};

BaseOperator.prototype.send = function(tuple, oldtuple) {
    if (tuple == oldtuple)
        var newtuple = tuple;
    else
        var newtuple = new Tuple({
            streamId: this.id,
            id: this.tupleOutCount,
            timestamp: oldtuple.timestamp,
            values: tuple
        });
        
    // call the partitioner to select the operator
    // if there is a partitioner
    var prefix = (this.partitioner) ? this.partitioner.do(newtuple) : '';
    
    this.outSocket.send(prefix + ' ' + JSON.stringify(newtuple));
    this.tupleOutCount++;
};

BaseOperator.prototype.subscribeTo = function(id) {
    this.inputs.push(id);
    return this;
};

BaseOperator.prototype.tupleWindow = function(size, advance) {
    this.window = true;
    this.windowSize = size;
    this.windowAdvance = (typeof(advance) == 'undefined') ? this.windowAdvance : advance;
    this.windowType = BaseOperator.windowTypes.TUPLE;
    this.buffer = new cbuffer(this.windowSize);
    return this;
};

BaseOperator.prototype.roundRobinPartitioner = function() {
    this.partitionerClass = partitioners.RoundRobinPartitioner; 
    return this;
};

BaseOperator.prototype.fieldPartitioner = function(field) {
    this.partitionerClass = partitioners.FieldPartitioner;
    this.partitionerOpts.field = field;
    return this;
};

BaseOperator.prototype.customPartitioner = function(ref, options) {
    this.partitionerClass = ref;
    this.partitionerOpts  = options;
    return this;
};

BaseOperator.prototype.placeAt = function(nodeId) {
    this.nodePlacement = nodeId;
    return this;
};

BaseOperator.prototype.error = function(msg) {
    var error = {
        type: 'error',
        message: msg,
        operator: this.id
    };
    
    this.logger.error(msg);
    process.send(JSON.stringify(error));
};

BaseOperator.prototype.reportStatus = function() {
    var status = {
        type: 'status',
        in: this.tupleInCount,
        out: this.tupleOutCount,
        ts: (new Date).getTime(),
        op: this.id
    };

    process.send(JSON.stringify(status));
};

BaseOperator.prototype.getLogConfig = function(nodeId) {
    return { type: 'file', filename: path.resolve(__dirname, '../../../logs/'+nodeId+'.worker-'+this.id+'.log') };
};

BaseOperator.windowTypes = {
    TUPLE: 0,
    TIME : 1
};

module.exports = BaseOperator;

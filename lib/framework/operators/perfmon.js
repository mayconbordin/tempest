var BaseOperator = require('./base.js');

/**
 * @class PerfMon
 * @classdesc Monitors the performance of the application (latency and throughput)
 *            Writes output to the logger.
 **/
var PerfMon = function() {
    this.latencyMeter = null;
    this.latencyMeterInterval = 100;
    
    this.currentTuple = null;
    this.latestTupleId = null;
    
    this.latencies = [];
};

PerfMon.prototype = new BaseOperator;

PerfMon.prototype.init = function(opts) {
    var self = this;
    
    // won't parse data
    opts.rawData = true;

    BaseOperator.prototype.init.call(this, opts);
    
    // every 1/2s read the current tuple timestamp
    // and calculate the latency
    // the stream generator and this operator MUST be in the same node
    // TODO: should not parse all tuples, only when measuring latency
    // TODO: should collect a sample of tuples a calculate the average latency
    this.latencyMeter = setInterval(function() {
        try {
            var currTuple = JSON.parse(self.currentTuple);
            
            if (currTuple.id != self.latestTupleId) {
                var currTime = (new Date).getTime();
                var diffTime = currTime - currTuple.timestamp;
                
                //self.logger.info('Tuple latency:', diffTime, 'ms');
                self.latencies.push(diffTime);
                
                self.latestTupleId = currTuple.id;
            }
        } catch(e) {
            self.logger.error('latencyMeter error:', e.message);
        }
    }, this.latencyMeterInterval);
};

PerfMon.prototype.reportStatus = function() {
    var status = {
        type: 'status',
        in: this.tupleInCount,
        out: this.tupleOutCount,
        lat: this.getCurrentLatency(),
        ts: (new Date).getTime(),
        op: this.id
    };

    process.send(JSON.stringify(status));
};

/**
 * Calculate the average tuple latency based on the collected tuples
 * and clear the list of collected tuples.
 * @return {number}
 **/
PerfMon.prototype.getCurrentLatency = function() {
    var sum = 0;
    this.latencies.forEach(function(lat) { sum += lat; });
    var avg = sum/this.latencies.length;
    this.latencies = [];
    
    return avg;
};

PerfMon.prototype.execute = function(tuple) {
    this.currentTuple = tuple;
};

module.exports = PerfMon;

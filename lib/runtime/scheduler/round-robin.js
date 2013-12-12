var Tempest = require('../../framework/tempest.js')
  , fn      = require('../../functions.js')
  ;

/**
 * @class RoundRobinScheduler
 * @classdesc Schedules operator instances to slaves in a round-robin fashion,
 *            leveling the number of instances in each slave.
 * @param {object} slaves The configuration of the slaves
 */
var RoundRobinScheduler = function(slaves) {
    var self = this;
    
    this.slaves    = [];
    this.numSlaves = slaves.nodes.length;
    this.nextSlave = 0;
    this._slaves   = slaves;
};

RoundRobinScheduler.prototype = {
    /**
     * Resets some variables before doing a new scheduling
     **/
    reset: function() {
        var self = this;
        
        this.slaves    = [];
        this.nextSlave = 0;
        
        this._slaves.nodes.forEach(function(_slave) {
            var slave = {id: _slave.id, addr: _slave.addr};
            slave.workerPort = _slave.workerPort || self._slaves.workerPort;
            slave.operatorCount = 0;
            
            self.slaves.push(slave);
        });
    },
    
    /**
     * Create a placement plan based on an application graph
     * @param {object} graph The graph with each operator instance as a member of
     *                       the graph
     * @return {object} The assignment of each instance to a slave (placement plan)
     **/
    schedule: function(graph) {
        var self = this;
        var plan = {};
        var instances = {};
        
        // clean up things before scheduling
        this.reset();
        
        // this one assigns each operator instance to a slave
        // creating a plan, but without the inputs assigned, only the output
        fn.foreach(graph, function(op, opId) {
            var slave = null, output = null, endpoint = null;
            
            // if the user specified a node, use it, otherwise chose one
            if (op.nodePlacement != null) {
                slave = self.getSlave(op.nodePlacement);
            } else {
                slave = self.getNextSlave();
            }
            
            // the output will be used by the operator itself
            // while the endpoint is used by the subscribers
            if (op.type != Tempest.elementTypes.Sink) {
                output = 'tcp://*:' + slave.workerPort;
                endpoint = 'tcp://' + slave.addr + ':' + slave.workerPort;
                slave.workerPort++;
            }
            
            if (!plan[slave.id]) {
                plan[slave.id] = [];
            }
                
            var opPlan = {id: opId, input: [], output: output, endpoint: endpoint, operator: op};
            
            // add the operator instance to the plan
            plan[slave.id].push(opPlan);
            
            // also add it to the list of operators for search
            instances[opId] = opPlan;
            
            slave.operatorCount++;
        });
        
        // now that every operator instance has an output assigned we can fill
        // up the inputs and the subscribers list
        fn.foreach(instances, function(instance) {
            instance.operator.input.forEach(function(op) {
                var out = instances[op].endpoint;
                instance.input.push({addr: out, partitioner: graph[op].partitioner});
            });
        });

        return plan;
    },
    
    /**
     * Get the next slave that has the least number of operators assigned to it
     * @return {object} the chosen slave
     **/
    getNextSlave: function() { 
        var slave = this.slaves[this.nextSlave++ % this.numSlaves];

        for (var i=0; i<this.slaves.length; i++)
            if (this.slaves[i].operatorCount < slave.operatorCount)
                slave = this.slaves[i];
        
        return slave;
    },
    
    /**
     * Get a slave by his id
     * @param {string} id The id of the slaveo
     * @return {object} The slave with the specified id or null if not found
     **/
    getSlave: function(id) {
        var chosen = null;
        
        for (var i=0; i<this.slaves.length; i++) {
            if (this.slaves[i].id == id) {
                chosen = this.slaves[i];
                break;
            }
        }
        
        return chosen;
    }
};

module.exports = RoundRobinScheduler;

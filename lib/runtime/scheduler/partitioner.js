var fn = require('../../functions.js');

var Partitioner = function() {};

Partitioner.prototype = {
    partition: function(graph) {
        var newGraph = {};

        // create a new graph the operators instances
        fn.foreach(graph, function(op, opId) {
            var instances = [];
            graph[opId].instances = instances;
            
            for (var i=0; i<op.count; i++) {
                var id = opId + ((op.count != 1) ? ('-' + i) : '');
                newGraph[id] = fn.clone(op);
                newGraph[id].instances = instances;
                newGraph[id].subscribers = [];
                newGraph[id].name = opId;
                instances.push(id);
            }
        });
        
        // add the input instances to each downstream operator
        // add the subscribers to each upstream operator
        fn.foreach(newGraph, function(op, opId) {
            var inputs = [];
            op.input.forEach(function(input) {
                graph[input].instances.forEach(function(instance) {
                    inputs.push(instance);
                    newGraph[instance].subscribers.push(opId);
                });
            });
            op.input = inputs;
        });
        
        return newGraph;
    }
};

module.exports = Partitioner;

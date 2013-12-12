var log4js       = require('log4js')
  , fn           = require('../functions.js')
  , BaseOperator = require('./operators/base.js')
  ;

/**
 * @class Tempest
 * @classdesc Build the application graph and start the specified operators
 *
 * @param {object} exports The module.exports object, to export the graph
 * @param {object} env     The process.env object, to receive the operator configuration
 **/
var Tempest = function(exports, env) {
    this.elements = {};
    this.graph = {};
    
    this.exports = exports;
    this.env = env;
};

Tempest.prototype = {
    setStream: function(id, obj) {
        return this.addElement(id, obj, 1, Tempest.elementTypes.STREAM);
    },

    setOperator: function(id, obj, numTasks) {
        return this.addElement(id, obj, numTasks, Tempest.elementTypes.OPERATOR);
    },

    setSink: function(id, obj, numTasks) {
        return this.addElement(id, obj, numTasks, Tempest.elementTypes.SINK);
    },

    addElement: function(id, obj, numTasks, type) {
        if (typeof(obj) == 'function')
            obj = new BaseOperator(obj);
        else if (!(obj instanceof BaseOperator))
            return false;
        
        obj.id = id;
        this.elements[id] = obj;
        this.graph[id]    = {type: type, input: obj.inputs, count: numTasks};
        
        return obj;
    },
    
    buildGraph: function() {
        var self = this;
        
        fn.foreach(this.graph, function(element, id) {
            element.partitioner   = (self.elements[id].partitionerClass != null);
            element.nodePlacement = self.elements[id].nodePlacement;
        });
    },

    start: function() {
        this.buildGraph();
        this.exports.graph = this.graph;

        // init the proper operator
        if (this.env.operator) {
            var config = JSON.parse(this.env.operator);
            
            // get the operator by its original name (w/o suffix)
            var op = this.elements[config.operator.name];
            
            // change its ID to the instance ID
            op.id = config.id;
            
            // configure file log
            if (this.env.logToFile) {
                log4js.configure({
                  appenders: [
                    op.getLogConfig(this.env.nodeId)
                  ]
                });
            }

            op.init(config);
        }
    }
};

Tempest.elementTypes = {
    STREAM: 0,
    OPERATOR: 1,
    SINK: 2
};

module.exports = Tempest;

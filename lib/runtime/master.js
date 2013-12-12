var log4js      = require('log4js')
  , zerorpc     = require("zerorpc")
  , fn          = require('../functions.js')
  , Tempest     = require('../framework/tempest.js')
  , Partitioner = require('./scheduler/partitioner.js')
  ;

var Master = function(slaves, scheduler) {
    this.app = {
        status: Master.appStatus.NONE,
        module: null,
        graph: null,
        instances: null,
        plan: null
    };
    
    this.slaves       = fn.clone(slaves);
    this.nodes        = this.slaves.nodes; 
    this.scheduler    = new scheduler(this.slaves);
    this.partitioner  = new Partitioner();
    
    this.status       = {};
    
    this.logger       = log4js.getLogger('master');
    this.statusLogger = log4js.getLogger('status');
    
    this.replyClient  = null;
    this.lostClient   = false;
};

Master.appStatus = {
    NONE:    0,
    LOADED:  1,
    RUNNING: 2,
    STOPPED: 3,
    FAILED:  4
};

Master.prototype = {
    connect: function() {
        var self = this;
        this.nodes.forEach(function(node) {
            self.logger.info('Connecting to ' + node.id);
            
            node.client = new zerorpc.Client();
            node.client.connect('tcp://' + node.addr + ':' + node.rpcPort);
            
            node.client.invoke("connect", function(error, status_str, more) {
                self.handleNodeResponse(node, error, status_str, more);
            });
        });
    },
    
    // TODO: make the reply work, need to send error to client
    submitApp: function(code, reply) {
        var self = this;
        
        try {
            this.app.module = fn.requireFromString(code);
        } catch (e) {
            //reply(e, null, true);
            this.logger.error('Application code error:', e.message);
            console.log(e.stack);
            return;
        }
            
        try {
            this.app.graph  = this.app.module.graph;
            this.app.instances = this.partitioner.partition(this.app.graph);
            this.app.plan = this.scheduler.schedule(this.app.instances);
        } catch (e) {
            this.logger.error('Application code error:', e.message);
            console.log(e.stack);
            return;
        }
        
        /*
        var pd = require('pretty-data').pd;
        var json_pp = pd.json(JSON.stringify(this.app.plan));
        console.log(json_pp);
        
        return;*/
        
        this.nodes.forEach(function(node) {
            self.logger.info('Loading application at ' + node.id);
            
            node.client.invoke("load_app", code, function(error, status_str, more) {
                if (error) {
                    self.app.status = Master.appStatus.FAILED;
                    self.logger.error('Error while loading application at ' + node.id + ': ' + error.message);
                } else {
                    self.app.status = Master.appStatus.LOADED;
                    self.logger.info('Application loaded at ' + node.id);
                }
            });
        });
    },
    
    startApp: function(reply) {
        var self = this;
        
        this.nodes.forEach(function(node) {
            self.logger.info('Starting application at ' + node.id);
            
            var nodePlan = self.app.plan[node.id];

            node.client.invoke("start_app", JSON.stringify(nodePlan), function(error, status_str, more) {
                if (error) {
                    self.app.status = Master.appStatus.FAILED;
                    self.stopApp();
                    self.logger.error('Error while starting application at ' + node.id + ': ' + error.message);
                } else {
                    self.app.status = Master.appStatus.RUNNING;
                    self.logger.info('Application started at ' + node.id);
                }
            });
        });
        
        reply(null, JSON.stringify({status: 'started'}), true);
        this.replyClient = reply;
    },
    
    stopApp: function() {
        var self = this;
        if (this.app.status == Master.appStatus.NONE) return;
    
        this.nodes.forEach(function(node) {
            node.client.invoke("stop_app", function(error, status, more) {
                if (error) {
                    self.app.status = Master.appStatus.FAILED;
                    self.logger.error('Error while stoping application at ' + node.id + ': ' + error.message);
                } else {
                    self.app.status = Master.appStatus.STOPPED;
                    self.logger.info('Application stopped at ' + node.id);
                }
            });
        });
    },
    
    handleNodeResponse: function(node, error, status_str, more) {
        var status = (typeof(status_str) == 'undefined') ? null : JSON.parse(status_str);
            
        if (error) {
            this.logger.error(error.message);
            
            // in case of error, stop application in slaves
            this.stopApp();
        }
        
        else if (status && status.counter == 0) {
            this.logger.info('Connected to ' + node.id);
        }
        
        else if (status) {
            if (status.status.length > 0) {
                var summary = this.statusSummary(status.status);
                
                // send the summary to the client
                if (!this.lostClient) {
                    try {
                        this.replyClient(null, JSON.stringify(summary), true);
                    } catch(e) {
                        this.logger.warn('Error with client connection:', e.message);
                        this.lostClient = true;
                    }
                }
            }

            // save the full log to a file
            this.statusLogger.info(status_str);
            
            // save current status
            node.status = status;
            
            // could set a timeout to check if received another heartbeat
            // timeout = time between heartbeats + tolerance time (latency, etc)
        }
    },
    
    statusSummary: function(status) {
        var self = this;
        var summary = {tp: {}};
        var zeroTp = 0;
        
        // send a summary to the client
        // could be done at a worker thread (in the future)
        fn.foreach(status, function(opStat) {
            var operator = (opStat.op.indexOf('-') == -1) ? opStat.op
                           : opStat.op.substr(0, opStat.op.indexOf('-'));
            
            if (!(opStat.op in self.status)) {
                self.status[opStat.op] = opStat;
            } else {
                var numTuples = opStat.in - self.status[opStat.op].in;
                var time = opStat.ts - self.status[opStat.op].ts;
                var tp = Math.floor((numTuples/time)*1000); // tuples/s

                self.status[opStat.op] = opStat;
                
                if (tp == 0) zeroTp++;
                
                if (operator in summary.tp) {
                    summary.tp[operator] += tp;
                } else {
                    summary.tp[operator] = tp;
                }
                
                if ('lat' in opStat) summary.lat = opStat.lat;
                if ('sz' in opStat) summary.sz = opStat.sz;
            }
        });
        
        summary.status = (zeroTp == status.length) ? 'done' : 'running';
        return summary;
    }
};

module.exports = Master;

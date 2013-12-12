var cluster = require('cluster')
  , log4js  = require('log4js')
  , fs      = require('fs')
  , path    = require('path')
  , fn      = require('../functions.js')
  ;

var Daemon = function(options) {
    this.workers = {};
    this.plan = null;
    
    // heartbeat
    this.interval = null;
    this.counter = 0;
    this.replyServer = null;
    
    // send cpu,mem usage
    this.status = [];
    
    this.logger = log4js.getLogger('daemon');
    
    this.heartbeatInterval = options.heartbeatInterval;
    this.nodeId            = options.nodeId;
    this.logToFile         = options.logToFile;
};

Daemon.prototype = {
    saveApp: function(code, reply) {
        try {
            var appFile = path.resolve(__dirname, '../../bin/user_app.js');
            fs.writeFileSync(appFile, code);
            cluster.setupMaster({
                exec : appFile,
                silent : false
            });
            this.logger.info('Application received and loaded');
        } catch(e) {
            this.logger.error('Error writing application to file: ', e);
            reply(e);
        }
    },
    
    deployApp: function(plan, reply) {
        var self = this;
        this.plan = plan;
        
        // start the workers
        this.logger.info('Starting the workers');
        this.logger.info('num. of workers: ' + plan.length);
        
        plan.forEach(function(op) {
            self.logger.info('Starting worker for operator ' + op.id);
            
            try {
                var worker = cluster.fork({
                    operator: JSON.stringify(op),
                    nodeId: self.nodeId,
                    logToFile: self.logToFile
                });

                worker.on('message', function(msg_str) {
                    self.messageHandler(msg_str);
                });

                worker.on('exit', function(code, signal) {
                    if(signal) {
                        self.logger.warn('Worker', op.id, 'was killed by signal:', signal);
                    } else if(code !== 0) {
                        self.logger.error('Worker', op.id, 'exited with error code:', code);
                    } else {
                        self.logger.info('Worker', op.id, 'finished his job');
                    }
                });

                self.workers[op.id] = worker;
            } catch (e) {
                self.logger.error('Unable do spawn worker:', e.message);
                reply(JSON.stringify(e));
            }
        });
        
        reply();
    },
    
    stopApp: function() {
        fn.foreach(this.workers, function(worker, id) {
            worker.kill();
        });

        this.workers = {};
        this.logger.info('Application stopped');
    },
    
    startHeartbeat: function(reply) {
        var self = this;
        
        if (this.interval) return;
        
        this.replyServer = reply;
        
        this.interval = setInterval(function() {
            try {
                reply(null, JSON.stringify({
                    status: self.status,
                    counter: self.counter++,
                    node: self.nodeId
                }), true);
                self.status = [];
            } catch(e) {
                clearTimeout(self.interval);
            }
        }, this.heartbeatInterval);
    },
    
    messageHandler: function(msg_str) {
        var msg = JSON.parse(msg_str);

        if (msg.type == 'error') {
            clearTimeout(this.interval);
            this.replyServer(msg.message, this.counter, false);
            
            this.stopApp();
            this.logger.warn('The application has been removed due to an error');
        }
        
        else if (msg.type == 'init') {
            // operator initialized
            this.logger.info('Operator', msg.op, 'initialized.');
        }
        
        else if (msg.type == 'status') {
            //this.logger.info('Operator', msg.op, 'received', msg.in, 'tuples and sended', msg.out, 'tuples');
            //console.log('STATUS');
            //this.statusLogger.info(msg_str);
            this.status.push(msg);
        }
        
        // message types:
        // error
        // init (instead of status)
        // status (num. of tuples per sec)
    }
};

module.exports = Daemon;

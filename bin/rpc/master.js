/**
 * Libraries
 **/
var zerorpc   = require("zerorpc")
  , log4js    = require('log4js')
  , config    = require('../../config/default.js')
  , Master    = require('../../lib/runtime/master.js')
  , scheduler = require('../../lib/runtime/scheduler/round-robin.js')
  ;
 
/**
 * Arguments
 **/
var logToFile  = process.argv[2] ? true : false;

/**
 * Log Configuration
 **/
var logAppenders = [config.logs.status];
logToFile ? logAppenders.push(config.logs.master.file) : logAppenders.push(config.logs.master.console);
log4js.configure({ appenders: logAppenders }); 
 
/**
 * Globals
 **/
var logger = log4js.getLogger('master');
var master = new Master(config.slaves, scheduler);

logger.info("Starting the master");
master.connect();

/**
 * RPC Server
 **/
var server = new zerorpc.Server({
    submit_app: function(code, reply) {
        master.submitApp(code, reply);
        reply();
    },
    
    start_app: function(reply) {
        master.startApp(reply);
        //reply();
    },
    
    stop_app: function(reply) {
        master.stopApp();
        reply();
    },
    
    status_app: function(reply) {
        var statuses = [ 'none', 'loaded', 'running', 'stopped', 'failed' ];
        var response = {status: statuses[master.app.status]};
        
        reply(null, JSON.stringify(response), false);
    },
    
    get_graph: function(reply) {
        reply(null, JSON.stringify(master.app.graph), false);
    },
    
    get_plan: function(reply) {
        reply(null, JSON.stringify(master.app.plan), false);
    }
});

server.bind('tcp://0.0.0.0:' + config.master.rpcPort);
server.on('error', function(error) {
    logger.error('RPC server error:', error);
});


/**
 * Startup Messaging
 **/
logger.info("PID: " + process.pid);
logger.info("Listening to RPC connections at port " + config.master.rpcPort);

var zerorpc = require('zerorpc')
  , log4js  = require('log4js')
  , config  = require('../../config/default.js')
  , Daemon  = require('../../lib/runtime/daemon.js')
  ;
  

/**
 * Arguments
 **/
var nodeId     = process.argv[2];
var rpcPort    = process.argv[3] || config.slaves.rpcPort;
var logToFile  = process.argv[4] ? true : false;

/**
 * Log Configuration
 **/
var logAppenders = logToFile ? [config.logs.daemon.file(nodeId)]
                             : [config.logs.daemon.console];
log4js.configure({ appenders: logAppenders }); 


/**
 * Globals
 **/
var logger = log4js.getLogger('daemon');
var daemon = new Daemon({
    heartbeatInterval: config.heartbeatInterval,
    nodeId: nodeId,
    logToFile: logToFile
});

/**
 * RPC Server
 **/
var server = new zerorpc.Server({
    connect: function(reply) {
        logger.info('Connected to server');
        daemon.startHeartbeat(reply);
    },
    
    load_app: function(code, reply) {
        daemon.saveApp(code, reply);
        reply();
    },
    
    start_app: function(plan_str, reply) {
        daemon.deployApp(JSON.parse(plan_str), reply);
        //reply();
    },
    
    stop_app: function(reply) {
        daemon.stopApp();
        reply();
    }
});

server.bind('tcp://0.0.0.0:' + rpcPort);
server.on('error', function(error) {
    logger.error('RPC server error:', error);
});


/**
 * Startup Messaging
 **/
logger.info("Daemon started");
logger.info("PID: " + process.pid);
logger.info("Listening to RPC connections at port " + rpcPort);

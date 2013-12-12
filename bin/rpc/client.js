var fs      = require('fs')
  , zerorpc = require("zerorpc")
  , log4js  = require('log4js')
  , config  = require('../../config/default.js')
  , fn      = require('../../lib/functions.js')
  ;
  
var logger = log4js.getLogger();
var client = new zerorpc.Client();
client.connect("tcp://127.0.0.1:" + config.master.rpcPort);

var cmd = process.argv[2];
var app = process.argv[3];

if (cmd == 'submit') {
    var file = fs.readFileSync(app, "utf8");
    client.invoke("submit_app", file, function(error, res, more) {
        if (error) logger.error('Error:', error.message);
        else logger.info('Application submitted');
        
        client.close();
    });
}

else if (cmd == 'start') {
    client.invoke("start_app", function(error, res, more) {
        if (error) {
            logger.error('Error:', error.message);
            client.close();
        } else {
            var response = JSON.parse(res);
            
            if (response.status == 'started')
                logger.info('Application started');
            else if (response.status == 'running') {
                if ('lat' in response)
                    logger.info('Latency:', parseFloat(response.lat).toFixed(3), 'ms');
                if ('sz' in response)
                    logger.info('Data received:', fn.humanFileSize(response.sz));
                /*fn.foreach(response.tp, function(tp, op) {
                    logger.info(op, 'throughput:', tp, 'tuples/s');
                });*/
            } else if (response.status == 'done') {
                logger.info('Application finished');
                client.close();
            }
        }
    });
}

else if (cmd == 'stop') {
    client.invoke("stop_app", function(error, res, more) {
        if (error) logger.error('Error:', error.message);
        else logger.info('Application stopped');
        client.close();
    });
}

else if (cmd == 'status') {
    client.invoke("status_app", function(error, res, more) {
        if (error) logger.error('Error:', error.message);
        else logger.info(res);
        client.close();
    });
}

else if (cmd == 'graph') {
    client.invoke("get_graph", function(error, res, more) {
        if (error) logger.error('Error:', error.message);
        else logger.info(res);
        client.close();
    });
}


else if (cmd == 'plan') {
    client.invoke("get_plan", function(error, res, more) {
        if (error) logger.error('Error:', error.message);
        else logger.info(res);
        client.close();
    });
}

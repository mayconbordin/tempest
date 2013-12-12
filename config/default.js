var path = require('path');

var config = {};

config.master = {
    node: '127.0.0.1',
    rpcPort: '7000'
};

config.slaves = {
    // default start port for workers
    workerPort: 5000,
    
    // default rpc port
    rpcPort: 6000,
    
    // this will be used by the master
    // the script that starts the slaves is only going to
    // use the port number
    
    // only the address is mandatory
    // but for a local test you need to set different ports for
    // rpc and the workers
    nodes: [
        {id: 'node-0', addr: '127.0.0.1', rpcPort:6000, workerPort: 5000},
        {id: 'node-1', addr: '127.0.0.1', rpcPort:6001, workerPort: 5100},
        {id: 'node-2', addr: '127.0.0.1', rpcPort:6002, workerPort: 5200}
    ]
};

config.heartbeatInterval = 3000;

config.logs = {
    master: {
        file: { type: 'file', filename: path.resolve(__dirname, '../logs/master.log'), category: 'master' },
        console: { type: 'console', category: 'master' }
    },

    status: {
        type: 'file',
        filename: path.resolve(__dirname, '../logs/status.log'),
        category: 'status',
        layout: { type: "pattern", pattern: "%m" }
    },

    daemon: {
        file: function(nodeId) {
            return { type: 'file', filename: path.resolve(__dirname, '../logs/'+nodeId+'.daemon.log'), category: 'daemon' };
        },
        console: { type: 'console', category: 'daemon' }
    }
};

module.exports = config;

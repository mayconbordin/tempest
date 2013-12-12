var net      = require('net')
  , fs       = require('fs')
  , readline = require('readline')
  , stream   = require('stream')
  , fn       = require('../../lib/functions.js')
  ;

var server = net.createServer(function (socket) {
    console.log('server connected');
    
    var sizeRead = 0;
    var totalSize = 0;
    var ts = (new Date()).getTime();
    
    socket.setMaxListeners(0);
    
    fs.createReadStream('/tmp/tweets.dataset', {
        'bufferSize': 4 * 1024
    }).pipe(socket);
 
    socket.on('end', function() {
        console.log('server disconnected');
    });
    
    socket.on('error', function(error) {
        console.log('Error:', error);
        socket.destroy();
    });
})

server.listen(4000, function() {
    console.log("Server running at port 4000\n");
});

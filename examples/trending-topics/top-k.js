var zerorpc = require('zerorpc')
  , tempest = require('../lib/framework')
  , ops     = tempest.operators
  ;
  
function extract(tuple) {
    if (tuple[0].length > 1) {
        var obj = JSON.parse(tuple[0]);
        var hashtags = obj.text.match(/#(\w+)/g);
        if (hashtags && hashtags.length > 0)
            for (i in hashtags) {
                var hashtag = hashtags[i].replace('#', '').toUpperCase();
                this.send([hashtag, 1], tuple);
            }
    }
}

var t = new tempest.Tempest(module.exports, process.env);

t.setStream('stream', new ops.SocketStream({port: 4000}))
 .roundRobinPartitioner()
 .placeAt('node-0');

t.setOperator('extractor', extract, 5)
 .subscribeTo('stream')
 .fieldPartitioner(0);

t.setOperator('countmin', new ops.CountMin(1e-7, 0.05, 10), 5)
 .subscribeTo('extractor')
 .tupleWindow(2);

t.setSink('filesink', new ops.FileSink('../tmp/result.out', 1000), 1)
 .subscribeTo('countmin');
 
t.setSink('perfmon', new ops.PerfMon(), 1)
 .subscribeTo('countmin')
 .placeAt('node-0');

t.start();

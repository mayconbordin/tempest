var zerorpc = require('zerorpc')
  , tempest = require('../lib/framework')
  , ops     = tempest.operators
  ;
  
/* functions */
function split(tuple) {
    var self = this;
    var words = tuple[0].toString().match(/\S+/g);
    
    if (words && words.length > 0) {
        words.forEach(function(word) {
            self.send([word.replace(/[|&;$%@"<>()+,\.\[\]_:!?]/g, "").toUpperCase(), 1], tuple);
        });
    }
}

function count(tuples) {
    var self = this;
    var hashtable = {};
    
    tuples.forEach(function(tuple) {
        if (!hashtable[tuple[0]])
            hashtable[tuple[0]] = 1;
        else
            hashtable[tuple[0]]++;
    });
    
    for (word in hashtable) {
        self.send([word, hashtable[word]], tuples[0]);
    }
}
  
var t = new tempest.Tempest(module.exports, process.env);

t.setStream('stream', new ops.SocketStream({port: 4000}))
 .roundRobinPartitioner()
 .placeAt('node-0');

t.setOperator('split', split, 5)
 .subscribeTo('stream')
 .fieldPartitioner(0);

t.setOperator('count', count, 5)
 .subscribeTo('split')
 .tupleWindow(100);

t.setSink('filesink', new ops.FileSink('../tmp/result.out', 1000), 1)
 .subscribeTo('count');
 
t.setSink('perfmon', new ops.PerfMon(), 1)
 .subscribeTo('count')
 .placeAt('node-0');
 
t.start();

var base         = require('./base.js')
  , countmin     = require('./countmin.js')
  , socketsink   = require('./sinks/socket-sink.js')
  , filesink     = require('./sinks/file-sink.js')
  , socketstream = require('./streams/socket-stream.js')
  , perfmon      = require('./perfmon.js')
  ;

module.exports = {
    BaseOperator: base,
    CountMin: countmin,
    SocketSink: socketsink,
    FileSink: filesink,
    SocketStream: socketstream,
    PerfMon: perfmon
};

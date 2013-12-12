var Tuple = function(args) {
    this.streamId = args.streamId;
    this.id = args.id;
    // (new Date()).getTime()
    this.timestamp = args.timestamp;

    for (var i=0; i<args.values.length; i++) {
        this.push(args.values[i]);
    }
};

Tuple.prototype = new Array;

module.exports = Tuple;

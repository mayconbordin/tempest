var crypto = require('crypto');

/**
 * @class FieldPartitioner
 * @classdesc Distributes the tuples by doing a modulo of the hash of the chosen
 * field.
 *
 * @param {array}  subscribers The list of operators to partition the tuples
 * @param {object} options     Options passed by the application plan
 **/
var FieldPartitioner = function(subscribers, options) {
    this.subscribers = subscribers;
    this.options = options;
};

/**
 * Receives a tuples and decides to which operator it goes to based on the modulo
 * of the hash of the selected field
 *
 * @param  {object} [tuple=null] The tuple to be partitioned
 * @return {string} The id of the selected operator
 **/
FieldPartitioner.prototype.do = function(tuple) {
    var hash = crypto.createHash('md5')
                     .update(tuple[this.options.field])
                     .digest('hex');
    
    var hashNum = parseInt(hash, 16);
    return this.subscribers[hashNum % this.subscribers.length];
};

module.exports = FieldPartitioner;

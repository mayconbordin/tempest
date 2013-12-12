/**
 * @class RoundRobinPartitioner
 * @classdesc Distributes the tuples in a round-robin fashion
 *
 * @param {array}  subscribers    The list of operators to partition the tuples
 * @param {object} [options=null] Options passed by the application plan, in this class they are ignored
 **/
var RoundRobinPartitioner = function(subscribers, options) {
    this.subscribers = subscribers;
    this.options = options;
    this.current = 0;
};

/**
 * Receives a tuples and decides to which operator it goes to, it does not use
 * the tuple argument, but it is here for consistencies reasons
 *
 * @param  {object} [tuple=null] The tuple to be partitioned
 * @return {string} The id of the selected operator
 **/
RoundRobinPartitioner.prototype.do = function(tuple) {
    return this.subscribers[this.current++ % this.subscribers.length];
};

module.exports = RoundRobinPartitioner;

var field = require('./field.js')
  , rr    = require('./round-robin.js')
  ;
  
module.exports = {
    RoundRobinPartitioner: rr,
    FieldPartitioner: field
};

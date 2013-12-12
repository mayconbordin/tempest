var fs           = require('fs')
  , BaseOperator = require('../base.js')
  ;

/**
 * @class FileSink
 * @classdesc Receive tuples and writes them to a file (async) in JSON format
 *
 * @param {string} filepath       The path to the file where data will be save
 * @param {number} [bufferSize=1] The number of tuples to buffer before writing to file
 **/
var FileSink = function(filepath, bufferSize) {
    this.filepath = filepath;
    this.bufferSize = (typeof(bufferSize) == 'number') ? bufferSize : 1;
    
    this.buffer = '';
    this.bufferCurrSize = 0;
};

FileSink.prototype = new BaseOperator;

FileSink.prototype.execute = function(tuple) {
    var self = this;
    
    if (this.bufferCurrSize == this.bufferSize) {
        fs.appendFile(this.filepath, this.buffer, function (err) {
            if (err) {
                self.logger.error('Unable to write to', self.filepath, ':', err);
            }
            
            self.tupleOutCount += self.bufferSize;
        });
        
        this.buffer = '';
        this.bufferCurrSize = 0;
    } else {
        this.buffer += JSON.stringify(tuple) + '\n';
        this.bufferCurrSize++;
    }
};

module.exports = FileSink;

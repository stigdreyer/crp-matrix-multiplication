var LZString = {
  
  // private property
  _keyStr : "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=",
  
  compressToBase64 : function (input) {
    var output = "";
    var chr1, chr2, chr3, enc1, enc2, enc3, enc4;
    var i = 0;
    
    input = this.compress(input);
    
    while (i < input.length*2) {
      
      if (i%2==0) {
        chr1 = input.charCodeAt(i/2) >> 8;
        chr2 = input.charCodeAt(i/2) & 255;
        if (i/2+1 < input.length) 
          chr3 = input.charCodeAt(i/2+1) >> 8;
        else 
          chr3 = NaN;
      } else {
        chr1 = input.charCodeAt((i-1)/2) & 255;
        if ((i+1)/2 < input.length) {
          chr2 = input.charCodeAt((i+1)/2) >> 8;
          chr3 = input.charCodeAt((i+1)/2) & 255;
        } else 
          chr2=chr3=NaN;
      }
      i+=3;
      
      enc1 = chr1 >> 2;
      enc2 = ((chr1 & 3) << 4) | (chr2 >> 4);
      enc3 = ((chr2 & 15) << 2) | (chr3 >> 6);
      enc4 = chr3 & 63;
      
      if (isNaN(chr2)) {
        enc3 = enc4 = 64;
      } else if (isNaN(chr3)) {
        enc4 = 64;
      }
      
      output = output +
        this._keyStr.charAt(enc1) + this._keyStr.charAt(enc2) +
          this._keyStr.charAt(enc3) + this._keyStr.charAt(enc4);
      
    }
    
    return output;
  },
  
  decompressFromBase64 : function (input) {
    var output = "",
        ol = 0, 
        output_,
        chr1, chr2, chr3,
        enc1, enc2, enc3, enc4,
        i = 0;
    
    input = input.replace(/[^A-Za-z0-9\+\/\=]/g, "");
    
    while (i < input.length) {
      
      enc1 = this._keyStr.indexOf(input.charAt(i++));
      enc2 = this._keyStr.indexOf(input.charAt(i++));
      enc3 = this._keyStr.indexOf(input.charAt(i++));
      enc4 = this._keyStr.indexOf(input.charAt(i++));
      
      chr1 = (enc1 << 2) | (enc2 >> 4);
      chr2 = ((enc2 & 15) << 4) | (enc3 >> 2);
      chr3 = ((enc3 & 3) << 6) | enc4;
      
      if (ol%2==0) {
        output_ = chr1 << 8;
        flush = true;
        
        if (enc3 != 64) {
          output += String.fromCharCode(output_ | chr2);
          flush = false;
        }
        if (enc4 != 64) {
          output_ = chr3 << 8;
          flush = true;
        }
      } else {
        output = output + String.fromCharCode(output_ | chr1);
        flush = false;
        
        if (enc3 != 64) {
          output_ = chr2 << 8;
          flush = true;
        }
        if (enc4 != 64) {
          output += String.fromCharCode(output_ | chr3);
          flush = false;
        }
      }
      ol+=3;
    }
    
    return this.decompress(output);
    
  },

  compressToUTF16 : function (input) {
    var output = "",
        i,c,
        current,
        status = 0;
    
    input = this.compress(input);
    
    for (i=0 ; i<input.length ; i++) {
      c = input.charCodeAt(i);
      switch (status++) {
        case 0:
          output += String.fromCharCode((c >> 1)+32);
          current = (c & 1) << 14;
          break;
        case 1:
          output += String.fromCharCode((current + (c >> 2))+32);
          current = (c & 3) << 13;
          break;
        case 2:
          output += String.fromCharCode((current + (c >> 3))+32);
          current = (c & 7) << 12;
          break;
        case 3:
          output += String.fromCharCode((current + (c >> 4))+32);
          current = (c & 15) << 11;
          break;
        case 4:
          output += String.fromCharCode((current + (c >> 5))+32);
          current = (c & 31) << 10;
          break;
        case 5:
          output += String.fromCharCode((current + (c >> 6))+32);
          current = (c & 63) << 9;
          break;
        case 6:
          output += String.fromCharCode((current + (c >> 7))+32);
          current = (c & 127) << 8;
          break;
        case 7:
          output += String.fromCharCode((current + (c >> 8))+32);
          current = (c & 255) << 7;
          break;
        case 8:
          output += String.fromCharCode((current + (c >> 9))+32);
          current = (c & 511) << 6;
          break;
        case 9:
          output += String.fromCharCode((current + (c >> 10))+32);
          current = (c & 1023) << 5;
          break;
        case 10:
          output += String.fromCharCode((current + (c >> 11))+32);
          current = (c & 2047) << 4;
          break;
        case 11:
          output += String.fromCharCode((current + (c >> 12))+32);
          current = (c & 4095) << 3;
          break;
        case 12:
          output += String.fromCharCode((current + (c >> 13))+32);
          current = (c & 8191) << 2;
          break;
        case 13:
          output += String.fromCharCode((current + (c >> 14))+32);
          current = (c & 16383) << 1;
          break;
        case 14:
          output += String.fromCharCode((current + (c >> 15))+32, (c & 32767)+32);
          status = 0;
          break;
      }
    }
    
    return output + String.fromCharCode(current + 32);
  },
  

  decompressFromUTF16 : function (input) {
    var output = "",
        current,c,
        status=0,
        i = 0;
    
    while (i < input.length) {
      c = input.charCodeAt(i) - 32;
      
      switch (status++) {
        case 0:
          current = c << 1;
          break;
        case 1:
          output += String.fromCharCode(current | (c >> 14));
          current = (c&16383) << 2;
          break;
        case 2:
          output += String.fromCharCode(current | (c >> 13));
          current = (c&8191) << 3;
          break;
        case 3:
          output += String.fromCharCode(current | (c >> 12));
          current = (c&4095) << 4;
          break;
        case 4:
          output += String.fromCharCode(current | (c >> 11));
          current = (c&2047) << 5;
          break;
        case 5:
          output += String.fromCharCode(current | (c >> 10));
          current = (c&1023) << 6;
          break;
        case 6:
          output += String.fromCharCode(current | (c >> 9));
          current = (c&511) << 7;
          break;
        case 7:
          output += String.fromCharCode(current | (c >> 8));
          current = (c&255) << 8;
          break;
        case 8:
          output += String.fromCharCode(current | (c >> 7));
          current = (c&127) << 9;
          break;
        case 9:
          output += String.fromCharCode(current | (c >> 6));
          current = (c&63) << 10;
          break;
        case 10:
          output += String.fromCharCode(current | (c >> 5));
          current = (c&31) << 11;
          break;
        case 11:
          output += String.fromCharCode(current | (c >> 4));
          current = (c&15) << 12;
          break;
        case 12:
          output += String.fromCharCode(current | (c >> 3));
          current = (c&7) << 13;
          break;
        case 13:
          output += String.fromCharCode(current | (c >> 2));
          current = (c&3) << 14;
          break;
        case 14:
          output += String.fromCharCode(current | (c >> 1));
          current = (c&1) << 15;
          break;
        case 15:
          output += String.fromCharCode(current | c);
          status=0;
          break;
      }
      
      
      i++;
    }
    
    return this.decompress(output);
    //return output;
    
  },


  
  compress: function (uncompressed) {
    var i, value,
        context_dictionary= {},
        context_dictionaryToCreate= {},
        context_c="",
        context_wc="",
        context_w="",
        context_enlargeIn= 2, // Compensate for the first entry which should not count
        context_dictSize= 3,
        context_numBits= 2,
        context_result= "",
        context_data_string="", 
        context_data_val=0, 
        context_data_position=0,
        ii;
    
    for (ii = 0; ii < uncompressed.length; ii += 1) {
      context_c = uncompressed.charAt(ii);
      if (!context_dictionary.hasOwnProperty(context_c)) {
        context_dictionary[context_c] = context_dictSize++;
        context_dictionaryToCreate[context_c] = true;
      }
      
      context_wc = context_w + context_c;
      if (context_dictionary.hasOwnProperty(context_wc)) {
        context_w = context_wc;
      } else {
        if (context_dictionaryToCreate.hasOwnProperty(context_w)) {
          if (context_w.charCodeAt(0)<256) {
            for (i=0 ; i<context_numBits ; i++) {
              context_data_val = (context_data_val << 1);
              if (context_data_position == 15) {
                context_data_position = 0;
                context_data_string += String.fromCharCode(context_data_val);
                context_data_val = 0;
              } else {
                context_data_position++;
              }
            }
            value = context_w.charCodeAt(0);
            for (i=0 ; i<8 ; i++) {
              context_data_val = (context_data_val << 1) | (value&1);
              if (context_data_position == 15) {
                context_data_position = 0;
                context_data_string += String.fromCharCode(context_data_val);
                context_data_val = 0;
              } else {
                context_data_position++;
              }
              value = value >> 1;
            }
          } else {
            value = 1;
            for (i=0 ; i<context_numBits ; i++) {
              context_data_val = (context_data_val << 1) | value;
              if (context_data_position == 15) {
                context_data_position = 0;
                context_data_string += String.fromCharCode(context_data_val);
                context_data_val = 0;
              } else {
                context_data_position++;
              }
              value = 0;
            }
            value = context_w.charCodeAt(0);
            for (i=0 ; i<16 ; i++) {
              context_data_val = (context_data_val << 1) | (value&1);
              if (context_data_position == 15) {
                context_data_position = 0;
                context_data_string += String.fromCharCode(context_data_val);
                context_data_val = 0;
              } else {
                context_data_position++;
              }
              value = value >> 1;
            }
          }
          context_enlargeIn--;
          if (context_enlargeIn == 0) {
            context_enlargeIn = Math.pow(2, context_numBits);
            context_numBits++;
          }
          delete context_dictionaryToCreate[context_w];
        } else {
          value = context_dictionary[context_w];
          for (i=0 ; i<context_numBits ; i++) {
            context_data_val = (context_data_val << 1) | (value&1);
            if (context_data_position == 15) {
              context_data_position = 0;
              context_data_string += String.fromCharCode(context_data_val);
              context_data_val = 0;
            } else {
              context_data_position++;
            }
            value = value >> 1;
          }
          
          
        }
        context_enlargeIn--;
        if (context_enlargeIn == 0) {
          context_enlargeIn = Math.pow(2, context_numBits);
          context_numBits++;
        }
        // Add wc to the dictionary.
        context_dictionary[context_wc] = context_dictSize++;
        context_w = String(context_c);
      }
    }
    
    // Output the code for w.
    if (context_w !== "") {
      if (context_dictionaryToCreate.hasOwnProperty(context_w)) {
        if (context_w.charCodeAt(0)<256) {
          for (i=0 ; i<context_numBits ; i++) {
            context_data_val = (context_data_val << 1);
            if (context_data_position == 15) {
              context_data_position = 0;
              context_data_string += String.fromCharCode(context_data_val);
              context_data_val = 0;
            } else {
              context_data_position++;
            }
          }
          value = context_w.charCodeAt(0);
          for (i=0 ; i<8 ; i++) {
            context_data_val = (context_data_val << 1) | (value&1);
            if (context_data_position == 15) {
              context_data_position = 0;
              context_data_string += String.fromCharCode(context_data_val);
              context_data_val = 0;
            } else {
              context_data_position++;
            }
            value = value >> 1;
          }
        } else {
          value = 1;
          for (i=0 ; i<context_numBits ; i++) {
            context_data_val = (context_data_val << 1) | value;
            if (context_data_position == 15) {
              context_data_position = 0;
              context_data_string += String.fromCharCode(context_data_val);
              context_data_val = 0;
            } else {
              context_data_position++;
            }
            value = 0;
          }
          value = context_w.charCodeAt(0);
          for (i=0 ; i<16 ; i++) {
            context_data_val = (context_data_val << 1) | (value&1);
            if (context_data_position == 15) {
              context_data_position = 0;
              context_data_string += String.fromCharCode(context_data_val);
              context_data_val = 0;
            } else {
              context_data_position++;
            }
            value = value >> 1;
          }
        }
        context_enlargeIn--;
        if (context_enlargeIn == 0) {
          context_enlargeIn = Math.pow(2, context_numBits);
          context_numBits++;
        }
        delete context_dictionaryToCreate[context_w];
      } else {
        value = context_dictionary[context_w];
        for (i=0 ; i<context_numBits ; i++) {
          context_data_val = (context_data_val << 1) | (value&1);
          if (context_data_position == 15) {
            context_data_position = 0;
            context_data_string += String.fromCharCode(context_data_val);
            context_data_val = 0;
          } else {
            context_data_position++;
          }
          value = value >> 1;
        }
        
        
      }
      context_enlargeIn--;
      if (context_enlargeIn == 0) {
        context_enlargeIn = Math.pow(2, context_numBits);
        context_numBits++;
      }
    }
    
    // Mark the end of the stream
    value = 2;
    for (i=0 ; i<context_numBits ; i++) {
      context_data_val = (context_data_val << 1) | (value&1);
      if (context_data_position == 15) {
        context_data_position = 0;
        context_data_string += String.fromCharCode(context_data_val);
        context_data_val = 0;
      } else {
        context_data_position++;
      }
      value = value >> 1;
    }
    
    // Flush the last char
    while (true) {
      context_data_val = (context_data_val << 1);
      if (context_data_position == 15) {
        context_data_string += String.fromCharCode(context_data_val);
        break;
      }
      else context_data_position++;
    }
    return context_data_string;
  },
  
  decompress: function (compressed) {
    var dictionary = [],
        next,
        enlargeIn = 4,
        dictSize = 4,
        numBits = 3,
        entry = "",
        result = "",
        i,
        w,
        bits, resb, maxpower, power,
        c,
        errorCount=0,
        literal,
        data = {string:compressed, val:compressed.charCodeAt(0), position:32768, index:1};
    
    for (i = 0; i < 3; i += 1) {
      dictionary[i] = i;
    }
    
    bits = 0;
    maxpower = Math.pow(2,2);
    power=1;
    while (power!=maxpower) {
      resb = data.val & data.position;
      data.position >>= 1;
      if (data.position == 0) {
        data.position = 32768;
        data.val = data.string.charCodeAt(data.index++);
      }
      bits |= (resb>0 ? 1 : 0) * power;
      power <<= 1;
    }
    
    switch (next = bits) {
      case 0: 
          bits = 0;
          maxpower = Math.pow(2,8);
          power=1;
          while (power!=maxpower) {
            resb = data.val & data.position;
            data.position >>= 1;
            if (data.position == 0) {
              data.position = 32768;
              data.val = data.string.charCodeAt(data.index++);
            }
            bits |= (resb>0 ? 1 : 0) * power;
            power <<= 1;
          }
        c = String.fromCharCode(bits);
        break;
      case 1: 
          bits = 0;
          maxpower = Math.pow(2,16);
          power=1;
          while (power!=maxpower) {
            resb = data.val & data.position;
            data.position >>= 1;
            if (data.position == 0) {
              data.position = 32768;
              data.val = data.string.charCodeAt(data.index++);
            }
            bits |= (resb>0 ? 1 : 0) * power;
            power <<= 1;
          }
        c = String.fromCharCode(bits);
        break;
      case 2: 
        return "";
    }
    dictionary[3] = c;
    w = result = c;
    while (true) {
      bits = 0;
      maxpower = Math.pow(2,numBits);
      power=1;
      while (power!=maxpower) {
        resb = data.val & data.position;
        data.position >>= 1;
        if (data.position == 0) {
          data.position = 32768;
          data.val = data.string.charCodeAt(data.index++);
        }
        bits |= (resb>0 ? 1 : 0) * power;
        power <<= 1;
      }

      switch (c = bits) {
        case 0: 
          if (errorCount++ > 10000) return "Error";
          bits = 0;
          maxpower = Math.pow(2,8);
          power=1;
          while (power!=maxpower) {
            resb = data.val & data.position;
            data.position >>= 1;
            if (data.position == 0) {
              data.position = 32768;
              data.val = data.string.charCodeAt(data.index++);
            }
            bits |= (resb>0 ? 1 : 0) * power;
            power <<= 1;
          }

          dictionary[dictSize++] = String.fromCharCode(bits);
          c = dictSize-1;
          enlargeIn--;
          break;
        case 1: 
          bits = 0;
          maxpower = Math.pow(2,16);
          power=1;
          while (power!=maxpower) {
            resb = data.val & data.position;
            data.position >>= 1;
            if (data.position == 0) {
              data.position = 32768;
              data.val = data.string.charCodeAt(data.index++);
            }
            bits |= (resb>0 ? 1 : 0) * power;
            power <<= 1;
          }
          dictionary[dictSize++] = String.fromCharCode(bits);
          c = dictSize-1;
          enlargeIn--;
          break;
        case 2: 
          return result;
      }
      
      if (enlargeIn == 0) {
        enlargeIn = Math.pow(2, numBits);
        numBits++;
      }
      
      if (dictionary[c]) {
        entry = dictionary[c];
      } else {
        if (c === dictSize) {
          entry = w + w.charAt(0);
        } else {
          return null;
        }
      }
      result += entry;
      
      // Add w+entry[0] to the dictionary.
      dictionary[dictSize++] = w + entry.charAt(0);
      enlargeIn--;
      
      w = entry;
      
      if (enlargeIn == 0) {
        enlargeIn = Math.pow(2, numBits);
        numBits++;
      }
      
    }
  }
};

var fs = require('fs');
var lazy = require('lazy');
var levelup = require('levelup');
var leveldown = require('levelup');
var Stream = require('stream');

var AuthClient = require('crp-auth-client');
var JobClient = require('crp-job-client');
var JobProducerClient = require('crp-job-producer-client');

var extend = require('util')._extend;
var exec = require('child_process').exec;


exports = module.exports = function (options) {

  if (!options.credential) throw new Error('Need credentials');
  if (!options.matrixOne || !options.matrixTwo) throw new Error('Need files for both matrices');

  var stream = new Stream();

  var defaultOptions = { 
    bid: 1,
    program: fs.readFileSync('./lib/program.min.js', 'utf8')
  }

  var options = extend(options, defaultOptions);

  var jobClient = JobClient({
      credential: options.credential
    });

  jobClient.jobs.create({
      bid: options.bid,
      program: options.program
    }, afterJobCreated);

  function afterJobCreated(err, job) {
      if (err) throw err;

      var data_stream = JobProducerClient({
        credential: options.credential,
        jobId: job._id
      });

      var db = levelup('./tempresult.db');

      var sent = 0;
      var received = 0;

      data_stream.on('error', function (err) { stream.emit('error', err); });
      data_stream.on('fault', function (err) { stream.emit('fault', err); });

      var count_matrix_one = 0;
      var cluster = 0;
      var dataunit = [0, '', ''];

      finished_reading_matrix_one = false;

      var m = 0;
      var k = 0;
      var clusterPerLine = 0;

      require('fs').createReadStream(options.matrixOne)
      .on('data', function(chunk) {
          for (var i = 0; i < chunk.length; ++i)
            if (chunk[i] == 10) m++;
      })
      .on('end', function() {

        k = Math.sqrt(500000000 / (2 * m));

        if (k < m) {

          var denominatorFound = false;
          var distance = 0;
          while (!denominatorFound) {

            if(m % (k - distance) === 0) {
              k -= distance;
              denominatorFound = true;
            }
            else if(m % (k + distance) === 0) {
              k += distance;
              denominatorFound = true;
            }
            else {
              distance++;
            }

          }

        }
        else {
          k = m;
        }

        clusterPerLine = m / k;
        process.nextTick(createData);

      });

      function createData() {

        dataunit[1] = '';

        var lazy_stream_one = new lazy(fs.createReadStream(options.matrixOne))
        .on('end', function() {
          var compressed_string_one = LZString.compressToBase64(dataunit[1].toString());
          dataunit[1] = compressed_string_one;
          if(count_matrix_one === m) finished_reading_matrix_one = true;
          process.nextTick(combineMatrixOneAndTwo);
        })
        .lines
        .skip(count_matrix_one)
        .take(k)
        .forEach(function (line) {
          count_matrix_one++;

          var string = line.toString();
          string = string.replace(/\s+/g, '');

          if (count_matrix_one % k === 0) {
            dataunit[1] += string;
          }
          else {
            dataunit[1] += string + ';';
          }
        });

      }

      function combineMatrixOneAndTwo() {

        if(dataunit[1] == '') return; //Prevents strange mistake when lazy is using small files

        count_matrix_two = 0;

        var lazy_stream_two = new lazy(fs.createReadStream(options.matrixTwo))
        .on('end', function () {
          if(!finished_reading_matrix_one) process.nextTick(createData);
        })
        .lines
        .forEach(function (line) {
          count_matrix_two++;

          var string = line.toString();
          string = string.replace(/\s+/g, '')

          if (count_matrix_two % k === 0) {
            dataunit[2] += string;
            var compressed_string_two = LZString.compressToBase64(dataunit[2].toString());
            dataunit[2] = compressed_string_two;
            data_stream.write(dataunit);
            dataunit[2] = '';
            cluster++;
            dataunit[0] = cluster;
            sent++;
          }
          else {
             dataunit[2] += string + ';';
          }
        });

      }

      data_stream.on('result', function(data) {

        var result_lines = LZString.decompressFromBase64(data[1]);
        result_lines = result_lines.split(';');

        for (var i = 0; i < result_lines.length; i++) {
          var key = [(i + parseInt(data[0] / clusterPerLine) * k), data[0]];

          db.put(JSON.stringify(key), result_lines[i], [{keyEncoding: 'json'}]);
        };

        received++;
      });

      data_stream.once('end', function() {
        if (sent != received) stream.emit('fault', 'Did not receive all results');

        var line_count = 0;
        var line = '';

        db.createReadStream()
        .on('data', function (data) {

          var key = JSON.parse(data.key);

          if(key[0] !== line_count ) {
            stream.emit('data', line);
            line_count++;
            line = '';
          }

          if(line == '') line += data.value;
          else line += ',' + data.value;

        })
        .on('end', function () {

          stream.emit('data', line);

          leveldown.destroy('./tempresult.db', function(err) {
            if(!err) throw new Error('Could not delete temporary database');
            stream.emit('end');
          });

        })

      });
      
  }

  return stream;
}
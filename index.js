var fs = require('fs');
var lazy = require('lazy');
var levelup = require('levelup');
var leveldown = require('leveldown');
var Stream = require('stream');
var gzip = require('gzip-js');

var AuthClient = require('crp-auth-client');
var TaskClient = require('crp-task-client');
var TaskProducerClient = require('crp-task-producer-client');

var extend = require('util')._extend;


exports = module.exports = function (options) {

  if (!options.credential) throw new Error('Need credentials');
  if (!options.matrixOne || !options.matrixTwo) throw new Error('Need files for both matrices');

  var stream = new Stream();

  var defaultOptions = { 
    bid: 1e10,
    program: fs.readFileSync('./build/program.min.js', 'utf8')
  }

  var options = extend(options, defaultOptions);

  var taskClient = TaskClient({
      credential: options.credential
    });

  taskClient.tasks.create({
      bid: options.bid,
      program: options.program
    }, afterTaskCreated);

  function afterTaskCreated(err, task) {
      if (err) throw err;

      var data_stream = TaskProducerClient({
        credential: options.credential,
        taskId: task._id
      });

      console.log('Task ID: ', task._id); //Debug code

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

        k = parseInt(Math.sqrt(500000000 / (2 * m)));

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
            dataunit[1] = dataunit[1].toString();
            dataunit[2] = dataunit[2].toString();
            var input = gzip.zip(JSON.stringify(dataunit));
            var buffer = new Buffer(JSON.stringify(input));
            data_stream.write(buffer.toString('base64'));
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

        //var result_lines = LZString.decompressFromBase64(data[1]);
        //result_lines = result_lines.split(';');
        result_lines = data[1].split(';');

        for (var i = 0; i < result_lines.length; i++) {
          var key = [(i + parseInt(data[0] / clusterPerLine) * k), data[0]];

          db.put(JSON.stringify(key), result_lines[i], [{keyEncoding: 'json'}]);
        };
        
        received++;

        if(sent > 0) {
          if(sent === received) {
            process.nextTick(sendResults);
          }
        }

      });

      data_stream.once('end', sendResults);

      function sendResults () {
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

      }
      
  }

  return stream;
}
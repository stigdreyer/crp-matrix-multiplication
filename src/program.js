function Run(data) {

	data = new Buffer(data, 'base64');
	data = data.toString('utf8');	
	data = JSON.parse(data);
	data = gzip.unzip(data);
	data = new Buffer(data).toString();
	data = JSON.parse(data);

	var linesFromMatrixOne = data[1].split(";");
	var linesFromMatrixTwo = data[2].split(";");

	var string = '';
	
	for (var i = 0; i < linesFromMatrixOne.length; i++) {

		var lineMatrixOne = linesFromMatrixOne[i].split(",");

		var temp_array = [];
		for (var j = 0; j < linesFromMatrixTwo.length; j++) {
			var sum = 0;
			var lineMatrixTwo = linesFromMatrixTwo[j].split(',');
			if (lineMatrixOne.length != lineMatrixTwo.length) {
				throw "error: incompatible sizes for matrix multilication";
			}

			for (var k = 0; k < lineMatrixTwo.length; k++) {
				sum += parseInt(lineMatrixOne[k], 10) * parseInt(lineMatrixTwo[k], 10);
			}

			temp_array[j] = sum;
		}
		if(i < (linesFromMatrixOne.length - 1)) string += (temp_array.toString() + ';');
		else string += temp_array.toString();
	}

	return [data[0], '' + string];
}

var gzip = require('gzip-js');
module.exports = Run;
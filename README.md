# Matrix Multiplication Module for CrowdProcess

## About CrowdProcess

CrowdProcess is a distributed computing platform that runs on top of web browsers. Its much simpler than existing platforms, with the potential to be much more powerful.

## How to install

To install the module simply run this command in your terminal:

```bash
$ npm install git+https://github.com/stigdreyer/crp-matrix-multiplication.git
```

## How to use

An "example" speaks more than 1000 words:

```javascript

//Needed for the crp-auth-client API
if (! process.env.NODE_ENV) process.env.NODE_ENV = 'production';

var AuthClient = require('crp-auth-client');
var crpMatrixMultiplication = require('crp-matrix-multiplication');

//Replace email & password by your own CrowdProcess login information
AuthClient.login('email', 'password', function(err, credential) {
    if (err) throw err;

  	var options = {
	   credential: credential,
	   //Either file1.csv or file2.csv has to contain a transposed matrix
	   matrixOne: 'file1.csv',
	   matrixTwo: 'file2.csv'
	};

	var results = crpMatrixMultiplication(options);

	//Returns the multiplied matrix line by line
	results.on('data', function (line) {

		console.log(line);

	})
});

```

There is also a debug option that allows you to see the percentage of sent and received dataunits as well as the CrowdProcess task id. Simply add ```debug: true``` to your options.

## Gotchas

At the moment this module is only able to handle the multiplication of two square matrices. This will be changed in the future.
Furthermore, the nature of this module requires one of the two matrices to be supplied in a transposed form.
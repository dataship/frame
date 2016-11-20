var Benchmark = require('benchmark'),
	tape = require('tape'),
	Frame = require('../frame');

var suite = new Benchmark.Suite();

var pass = 0,
	fail = 0;

function randomIntArray(N, K){

	var data = [];

	for(var i = 0; i < N; i++){
		data.push(Math.random() * K | 0);
	}

	return data;
};

function randomFloatArray(N){

	var data = [];

	for(var i = 0; i < N; i++){
		data.push(Math.random() / Math.sqrt(N));
	}

	return data;
};

function logTapBegin(){
	console.log("TAP version 13");
}

function logTapEnd(length){
	console.log("\n1.." + suite.length);
	console.log("# tests " + suite.length);
	console.log("# pass  " + pass);
	if(fail)
		console.log("# fail  " + fail);
	else
		console.log("\n# ok\n");
}

/*
 * logTapResult(event, this)
 */
function logTapResult(event, benchmark, N){

	var pm = '\xb1',
		mu = '\xb5'
		size = benchmark.stats.sample.length;

	var gflops = benchmark.hz * N / 1e9;

	var info = Benchmark.formatNumber(gflops.toFixed(3)) + ' GFlops/sec ' +
		' ' + pm + benchmark.stats.rme.toFixed(2) + '% ' +
		' n = ' + size +
		' ' + mu + " = " + (benchmark.stats.mean * 1000).toFixed(0) + 'ms';

	//var info = "YES!";

	console.log("ok " + event.currentTarget.id + " " + benchmark.name);
	console.log("# " + info);
}

function logTapError(event, benchmark){

	console.log("not ok " + event.currentTarget.id + " " +  benchmark.name);
	// show error
	console.log("  ---");
	console.log("  error: " + benchmark.error);
	console.log("  ...");
}

function createBenchmark(N, K){

	var name = "groupby.sum: " + N + "x" + K;


	var b = new Benchmark(name, function(){
		var group = this.frame.groupby("group-col");
		var result = group.reduce("reduce-col");
	})// add listeners
	.on('start', function(event){
		// generate data
		var groupCol = randomIntArray(N, K);
		var valueCol = randomFloatArray(N);

		// create frame
		var columnDict = {
			"group-col" : groupCol,
			"reduce-col" : valueCol
		};

		this.frame = new Frame(columnDict);
	})
	.on('cycle', function(event) {
	})
	.on('complete', function(event) {
		if(this.error){
			logTapError(event, this);
			fail++;
		} else {
			logTapResult(event, this, N);
			pass++;
		}
	});

	return b;
}

logTapBegin();

suite.add(createBenchmark(100000, 20));

suite.on('complete', logTapEnd);

// run async
suite.run({ 'async': true });

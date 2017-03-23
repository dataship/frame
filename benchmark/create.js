var benchtap = require('benchtap'),
	gen = require('../generate'),
	Frame = require('../lib/frame');

function createSetup(N, K, useStrings){
	return function(event){
		// generate data
		this.groupCol = gen.Array.int(N, K);
		this.valueCol = gen.Array.int(N, 100);

		// map to strings
		if(useStrings)
			this.groupCol = this.groupCol.map(i => ["a", "b", "c"][i]);

	};
}

function test(){

	// create frame
	var columnDict = {
		"group-col" : this.groupCol,
		"reduce-col" : this.valueCol
	};

	this.frame = new Frame(columnDict);
}

var N = 100000,
	K = 3;

var name = "create: " + N + "x" + K;
benchtap(name, {"operations": N}, createSetup(N, K), test);


name += " (strings)";
benchtap(name, {"operations": N}, createSetup(N, K, true), test);



var N = 1000000;

name = "create: " + N + "x" + K;
benchtap(name, {"operations": N}, createSetup(N, K), test);


name += " (strings)";
benchtap(name, {"operations": N}, createSetup(N, K, true), test);

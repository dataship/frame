var benchtap = require('benchtap'),
	gen = require('../generate'),
	Frame = require('../frame');

function createSetup(N, K, useStrings){
	return function(event){
		// generate data
		var groupCol = gen.Array.int(N, K);
		var valueCol = gen.Array.int(N, 100);

		// map to strings
		if(useStrings)
			groupCol = groupCol.map(i => ["a", "b", "c"][i]);

		// create frame
		var columnDict = {
			"group-col" : groupCol,
			"reduce-col" : valueCol
		};

		this.frame = new Frame(columnDict);
	};
}

var N = 100000,
	K = 3;

var name = "groupby.sum: " + N + "x" + K;

benchtap(name, {"operations" :  2*N}, createSetup(N, K), function(){
	var group = this.frame.groupby("group-col");
	var result = group.reduce("reduce-col");
});

name += " (strings)";

benchtap(name, {"operations" :  2*N}, createSetup(N, K, true), function(){
	var group = this.frame.groupby("group-col");
	var result = group.reduce("reduce-col");
});

N = 1000000;
name = "groupby.sum: " + N + "x" + K;

benchtap(name, {"operations" :  2*N}, createSetup(N, K), function(){
	var group = this.frame.groupby("group-col");
	var result = group.reduce("reduce-col");
});

name += " (strings)";

benchtap(name, {"operations" :  2*N}, createSetup(N, K, true), function(){
	var group = this.frame.groupby("group-col");
	var result = group.reduce("reduce-col");
});

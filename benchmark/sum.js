var benchtap = require('benchtap'),
	gen = require('../generate'),
	Frame = require('../frame');

//var benchtap = new Benchtap();


function createSetup(N, K){
	return function(event){
		// generate data
		var groupCol = gen.Array.int(N, K);
		var valueCol = gen.Array.int(N, 100);

		// map to strings
		groupCol = groupCol.map(i => ["a", "b", "c"][i]);

		// create frame
		var columnDict = {
			"group-col" : groupCol,
			"reduce-col" : valueCol
		};

		this.frame = new Frame(columnDict);
		this.group = this.frame.groupby("group-col");
		console.log("setup done");
	};
}

var N = 100000,
	K = 3;

var name = "sum: " + N + "x" + K;

benchtap(name, {"operations": N}, createSetup(N, K),function(){
	var result = this.group.reduce("reduce-col");
	console.log(result);
});


/*
var N = 1000000;

name = "sum: " + N + "x" + K;

benchtap.add(name, createSetup(N, K),function(){
	var result = this.group.reduce("reduce-col");
}, N);

benchtap.run();
*/

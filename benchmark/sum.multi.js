var benchtap = require('benchtap'),
	gen = require('../generate'),
	Frame = require('../frame');

var STRINGS = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"];

function createSetup(N, K, M, useStrings){
	return function(event){
		// generate data
		var columns = {
			"value" : gen.Array.int(N, 100)
		};
		var names = [];
		for (var m = 0; m < M; m++){
			var name = "id_"+m;
			columns[name] = gen.Array.int(N, K);

			// map to strings
			if(useStrings){
				columns[name] = columns[name].map(i => STRINGS[i]);
			}

			names[m] = name;
		}


		// create frame
		this.frame = new Frame(columns);
		// group on all id columns
		this.group = this.frame.groupbymulti(names);
	};
}

var N = 100000,
	K = 3,
	M = 4;

var name = "sum.multi: " + N + "x" + K + "x" + M;

benchtap(name, {"operations": N}, createSetup(N, K, M), function(){
	var result = this.group.summulti("value");
});

/*
name += " (strings)";

benchtap(name, {"operations": N}, createSetup(N, K, true), function(){
	var result = this.group.reduce("reduce-col");
});
*/


var N = 1000000;

name = "sum.multi: " + N + "x" + K + "x" + M;

benchtap(name, {"operations": N}, createSetup(N, K, M), function(){
	var result = this.group.summulti("value");
});

/*
name += " (strings)";

benchtap(name, {"operations": N}, createSetup(N, K, true), function(){
	var result = this.group.reduce("reduce-col");
});
*/

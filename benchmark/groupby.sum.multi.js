var benchtap = require('benchtap'),
	gen = require('../generate'),
	Frame = require('../frame');

var STRINGS = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"];

// create a frame for multidimensional groupby
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
		//console.log(names);


		// create frame
		this.frame = new Frame(columns);
	};
}

var N = 100000,
	K = 3,
	M = 4;

var name = "groupby.sum.multi: " + N + "x" + K + "x" + M;

benchtap(name, {"operations" :  2*N}, createSetup(N, K, M), function(){
	//var group = this.frame.groupbymulti(["group-col0", "group-col1"]);
	var group = this.frame.groupbymulti(["id_0", "id_1", "id_2", "id_3"]);
	var result = group.summulti("value");
});

name += " (strings)";

benchtap(name, {"operations" :  2*N}, createSetup(N, K, M, true), function(){
	//var group = this.frame.groupbymulti(["group-col0", "group-col1"]);
	var group = this.frame.groupbymulti(["id_0", "id_1", "id_2", "id_3"]);
	var result = group.summulti("value");
});

N = 1000000;
name = "groupby.sum.multi: " + N + "x" + K + "x" + M;

benchtap(name, {"operations" :  2*N}, createSetup(N, K, M), function(){
	//var group = this.frame.groupbymulti(["group-col0", "group-col1"]);
	var group = this.frame.groupbymulti(["id_0", "id_1", "id_2", "id_3"]);
	var result = group.summulti("value");
});

name += " (strings)";

benchtap(name, {"operations" :  2*N}, createSetup(N, K, M, true), function(){
	//var group = this.frame.groupbymulti(["group-col0", "group-col1"]);
	var group = this.frame.groupbymulti(["id_0", "id_1", "id_2", "id_3"]);
	var result = group.summulti("value");
});

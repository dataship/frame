var benchtap = require('benchtap'),
	gen = require('../lib/test').generate,
	Frame = require('../lib/frame');

var STRINGS = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"];
/*
N - number of rows
K - number of distinct values in id columns
M - number of id columns
*/
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
		//this.frame.where(row => row["id_1"] == 1);
		//this.frame.where("id_1", id => id == 1);
	};
}

var N = 100000,
	K = 3,
	M = 2;

// var name = "where.function: " + N + "x" + K + "x" + M;
//
// benchtap(name, {"operations": N}, createSetup(N, K, M), function(){
// 	//var result = this.frame.where(row => row["id_0"] == 1);
// 	var result = this.frame.where("id_0", id => id == 1);
// });

/*
name += " (strings)";

benchtap(name, {"operations": N}, createSetup(N, K, true), function(){
	var result = this.group.reduce("reduce-col");
});
*/


// var N = 1000000;
//
// name = "where.function: " + N + "x" + K + "x" + M;
//
// benchtap(name, {"operations": N}, createSetup(N, K, M), function(){
// 	//var result = this.frame.where(row => row["id_0"] == 1);
// 	var result = this.frame.where("id_0", id => id == 1);
// });

/*
name += " (strings)";

benchtap(name, {"operations": N}, createSetup(N, K, true), function(){
	var result = this.group.reduce("reduce-col");
});
*/

N = 1000000;
K = 200;
M = 2;

// var name = "where.function: " + N + "x" + K + "x" + M;
//
// benchtap(name, {"operations": N}, createSetup(N, K, M), function(){
// 	//var result = this.frame.where(row => row["id_0"] == 1);
// 	var result = this.frame.where("id_0", id => id == 1);
// });

var name = "where.in: " + N + "x" + K + "x" + M;

benchtap(name, {"operations": N}, createSetup(N, K, M), function(){
	//var result = this.frame.where(row => row["id_0"] == 1);
	var result = this.frame.where("id_0", [0, 1, 3, 10, 12, 18, 101, 52, 23, 18, 7, 12, 154, 34, 117, 5]);
});

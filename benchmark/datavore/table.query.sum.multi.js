var benchtap = require('benchtap'),
	gen = require('../../generate'),
	dv = require('./datavore');

var STRINGS = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"];

function createSetup(N, K, M, useStrings){
	return function(event){

		var columns = [
			{"name" : "value", "type":"numeric", "values": gen.Array.int(N, 100)}
		];
		var names = [];
		for (var m = 0; m < M; m++){
			var name = "id_"+m;
			var column = {
				"name" : name,
				"type" : "ordinal",
				"values" : gen.Array.int(N, K)
			};

			// map to strings
			if(useStrings){
				column.values = column.values.map(i => STRINGS[i]);
			}

			columns.push(column);


			names[m] = name;
		}

		// create table
		this.table = dv.table(columns);

		// generate data
		/*
		this.table = dv.table();
		this.table.addColumn("group-col", groupCol, dv.type.nominal);
		this.table.addColumn("reduce-col", valueCol, dv.type.numeric);
		*/

	};
}


function test(){

	//var names = this.names;
	//"dims" : ["id_0", "id_1"],
	var result = this.table.query({
		"dims" : ["id_0", "id_1", "id_2", "id_3"],
		"vals" : [dv.sum("value")]
	});
}


var N = 100000,
	K = 3,
	M = 4;

var name = "table.query.sum.multi: " + N + "x" + K + "x" + M;
benchtap(name, {"operations" : 2*N}, createSetup(N, K, M), test);

name += " (strings)";
benchtap(name, {"operations" : 2*N}, createSetup(N, K, M, true), test);


var N = 1000000;

name = "table.query.sum.multi: " + N + "x" + K + "x" + M;
benchtap(name, {"operations" : 2*N}, createSetup(N, K, M), test);

name += " (strings)";
benchtap(name, {"operations" : 2*N}, createSetup(N, K, M, true), test);

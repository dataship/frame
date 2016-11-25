var benchtap = require('benchtap'),
	gen = require('../../generate'),
	dv = require('./datavore');

function createSetup(N, K, useStrings){
	return function(event){

		this.groupCol = gen.Array.int(N, K);
		this.valueCol = gen.Array.int(N, 100);

		if(useStrings)
			this.groupCol = this.groupCol.map(i => ["a", "b", "c"][i]);
	};
}

function test(){

	// create table
	var table = dv.table([
		{name:"group-col", type:"nominal", values:this.groupCol},
		{name:"reduce-col", type:"numeric", values:this.valueCol}
	]);
}


// 1 hundred thousand data points/rows
var N = 100000,
	K = 3;

var name = "table.query.sum: " + N + "x" + K;
benchtap(name, {"operations" : 2*N}, createSetup(N, K), test);

name += " (strings)";
benchtap(name, {"operations" : 2*N}, createSetup(N, K, true), test);

// 1 million data points/rows
var N = 1000000;

name = "table.query.sum: " + N + "x" + K;
benchtap(name, {"operations" : 2*N}, createSetup(N, K), test);

name += " (strings)";
benchtap(name, {"operations" : 2*N}, createSetup(N, K, true), test);

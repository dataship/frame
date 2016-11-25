var benchtap = require('benchtap'),
	gen = require('../../generate'),
	dv = require('./datavore');

function createSetup(N, K){
	return function(event){

		var groupCol = gen.Array.int(N, K);
		var valueCol = gen.Array.int(N, 100);

		//groupCol = groupCol.map(i => ["a", "b", "c"][i]);

		// create table
		this.table = dv.table([
			{name:"group-col", type:"nominal", values:groupCol},
			{name:"reduce-col", type:"numeric", values:valueCol}
		]);

		// generate data
		/*
		this.table = dv.table();
		this.table.addColumn("group-col", groupCol, dv.type.nominal);
		this.table.addColumn("reduce-col", valueCol, dv.type.numeric);
		*/

	};
}

function test(){

	var result = this.table.query({
		"dims" : [0],
		"vals" : [dv.sum("reduce-col")]
	});
}


var N = 100000,
	K = 3;

var name = "table.query.sum: " + N + "x" + K;

benchtap(name, {"operations" : 2*N}, createSetup(N, K), test);


var N = 1000000;

name = "table.query.sum: " + N + "x" + K;

benchtap(name, {"operations" : 2*N}, createSetup(N, K), test);

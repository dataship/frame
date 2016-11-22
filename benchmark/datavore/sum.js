var Benchtap = require('benchtap'),
	gen = require('../../generate'),
	dv = require('./datavore');

var benchtap = new Benchtap();


function createSetup(N, K){
	return function(event){
		// generate data
		var groupCol = gen.Array.int(N, K);
		var valueCol = gen.Array.float(N);

		groupCol = groupCol.map(i => ["a", "b", "c"][i]);

		// create table
		this.table = dv.table();
		this.table.addColumn("group-col", groupCol, dv.type.nominal);
		this.table.addColumn("reduce-col", valueCol, dv.type.numeric);

	};
}

function test(){

	var result = this.table.query({
		"dims" : ["group-col"],
		"vals" : [dv.sum("reduce-col")]
	});
}

var N = 100000,
	K = 3;

var name = "table.query.sum: " + N + "x" + K;

benchtap.add(name, createSetup(N, K), test, N);

var N = 1000000

name = "table.query.sum: " + N + "x" + K;

benchtap.add(name, createSetup(N, K), test, N);

benchtap.run();

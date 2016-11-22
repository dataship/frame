var Benchtap = require('benchtap'),
	gen = require('../../generate'),
	dv = require('./datavore');

var benchtap = new Benchtap();
function generate_data(N) {
    function randn(n) {
        return Math.max(0, Math.floor(n*(Math.random()-0.001)));
    }

    // generate synthetic data set
    var cols = [[],[]],  // data columns
        am = ["a","b","c"]; // domain of 1st col

    // generate rows from random data
    for (var i=0; i<N-1; ++i) {
        cols[0].push(am[randn(am.length)]);
        //cols[1].push(bm[randn(bm.length)]);
        cols[1].push(randn(10000));
    }

    /*
    // add one extra row to introduce sparsity
    cols[0].push("d");
    cols[1].push(1);
    cols[2].push(10);
    */

    // construct datavore table
    /*
    var names = ["a","b","x"],                   // column names
        types = ["nominal","nominal","numeric"]; // dv.type constants
    */

    var names = ["a", "x"],                   // column names
        types = ["nominal", "numeric"]; // dv.type constants
    return dv.table(cols.map(function(d,i) {
        return {name:names[i], type:types[i], values:d};
    }));
}

var N = 1e6;
var K = 3;

/*
var groupCol = gen.Array.int(N, K);
var valueCol = gen.Array.int(N, 10000);

groupCol = groupCol.map(i => ["a", "b", "c"][i]);

// create table
table = dv.table([
	{name:"group-col", type:"nominal", values:groupCol},
	{name:"reduce-col", type:"numeric", values:valueCol}
]);
*/
var table = generate_data(N);

function createSetup(N, K){
	return function(event){
		// generate data
		/*
		var groupCol = gen.Array.int(N, K);
		var valueCol = gen.Array.int(N, 10000);

		groupCol = groupCol.map(i => ["a", "b", "c"][i]);

		// create table
		this.table = dv.table([
			{name:"group-col", type:"nominal", values:groupCol},
			{name:"reduce-col", type:"numeric", values:valueCol}
		]);
		*/
		/*
		this.table = dv.table();
		this.table.addColumn("group-col", groupCol, dv.type.nominal);
		this.table.addColumn("reduce-col", valueCol, dv.type.numeric);
		*/

	};
}

function test(){

	var result = table.query({
		"dims" : [0],
		"vals" : [dv.sum("x")]
	});
}

/*
var N = 100000,
	K = 3;

var name = "table.query.sum: " + N + "x" + K;

benchtap.add(name, createSetup(N, K), test, N);

var K = 3;
var N = 1000000;
*/

name = "table.query.sum: " + N + "x" + K;

benchtap.add(name, createSetup(N, K), test, N);

benchtap.run();

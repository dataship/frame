var benchtap = require('benchtap'),
	gen = require('../generate'),
	Frame = require('../lib/frame');


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
	M = 1;

var groups = [];
for(var i = 0; i < M; i ++) groups.push("id_"+i);

var name = "groupby.sum: " + N + "x" + K + "x" + M;

benchtap(name, {"operations" :  2*N}, createSetup(N, K, M), function(){

	//var group = this.frame.groupbymulti(["group-col0", "group-col1"]);
	var group = this.frame.groupby(groups);
	var result = group.sum("value");
});

name += " (strings)";

benchtap(name, {"operations" :  2*N}, createSetup(N, K, M, true), function(){

	//var group = this.frame.groupbymulti(["group-col0", "group-col1"]);
	var group = this.frame.groupby(groups);
	var result = group.sum("value");
});

N = 1000000;
name = "groupby.sum: " + N + "x" + K + "x" + M;

benchtap(name, {"operations" :  2*N}, createSetup(N, K, M), function(){

	//var group = this.frame.groupbymulti(["group-col0", "group-col1"]);
	var group = this.frame.groupby(groups);
	var result = group.sum("value");
});

name += " (strings)";

benchtap(name, {"operations" :  2*N}, createSetup(N, K, M, true), function(){

	//var group = this.frame.groupbymulti(["group-col0", "group-col1"]);
	var group = this.frame.groupby(groups);
	var result = group.sum("value");
});

/*
var tests = [
	0
];

var RTOL = 1e-05, // 1e-05
	ATOL = 1e-12; // 1e-12

var dataDirectory = 'test/data/sum/',
	testFile = 'small.json';

var floader = require('floader'),
	dtest = require('../lib/test');

floader.load(dataDirectory + testFile, function(err, config){

	var suite = JSON.parse(config);

	for(var j = 0; j < tests.length; j++){

		var i = tests[j];
		var prefix = String("0000" + (i + 1)).slice(-4);

		// directory containing matrix data files for current test
		var directory = dataDirectory + prefix + '/';

		var test = suite[i];

		var names = test.id.map(function(spec, i){ return "id_" + i;});
		var types = test.id.map(function(spec, i){ return spec['type'];});

		var value_names = ["value_0"];
		var value_types = [test.value[0].type];

		var N = test.N; // number of rows
		var distincts = test.id.map(function(spec, i){ return spec.K; });

		var testName = "groupby.summulti: " + N + " x " + "(" + distincts.join(", ") + ")"
		//tape(testName, generateTestCase(directory, names, types, ["value_0"], [test.value[0].type]));

		//var name = "groupby.sum.multi: " + N + "x" + K + "x" + M;

		benchtap(testName, {"operations" :  2*N},
			createSetup(directory, names, types, value_names, value_types),
			function(event){

				var g = this.frame.groupbymulti(names);
				var actual = g.summulti(value_names[0]);

				event.resolve();
		});
	}
});

var OUT_FILENAME = "out.json";

function createSetup(directory, id_names, id_types, value_names, value_types){
	return function(event){

		var self = this;
		var names = id_names.concat(value_names);
		var types = id_types.concat(value_types);

		// which columns require a key file?
		var key_names = id_names.filter(function(item, i){
			return id_types[i] in dtest.string_types
		});
		var key_types = id_types.filter(function(item, i){
			return item in dtest.string_types
		});

		console.log(directory);
		// load columns from files
		dtest.load(directory, names, types, function(err, columns){

			if(err) return console.log(err);

			console.log("running setup.");
			// load key files
			dtest.load_key(directory, key_names, key_types, function(err, keys){

				floader.load(directory + OUT_FILENAME, function(err, out){
					var expected = JSON.parse(out);

					var column_set = {};
					for (var i = 0; i < names.length; i++){
						var name = names[i];
						var column = columns[i];
						column_set[name] = column;
					}
					// keys map a small set of integers to other things (like strings)
					// they're a very simple form of fixed length coding
					var key_set = {};
					for (var i = 0; i < keys.length; i++){
						var name = key_names[i];
						var key = keys[i];
						key_set[name] = key;
					}

					self.frame = new Frame(column_set, key_set);

					event.resolve();

				});

			});
		});
	};
}
*/

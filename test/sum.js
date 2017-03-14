var tape = require('tape'),
	Frame = require('../frame');
/*
tape("groupby.sum", function(t){
	t.plan(1);
	var frame = new Frame({
		"id"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	var expected = [10, 9];

	var g = frame.groupby("id");
	var actual = g.sum("value");

	t.equals(JSON.stringify(actual), JSON.stringify(expected), "reduce");

});

tape("groupby.sum.strings", function(t){
	t.plan(1);
	var frame = new Frame({
		"id"  : ["a", "a", "a", "b", "b", "a", "b", "a", "b"],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	var expected = [10, 9];

	var g = frame.groupby("id");
	var actual = g.sum("value");

	t.equals(JSON.stringify(actual), JSON.stringify(expected), "reduce");

});
*/
var dataDirectory = 'test/data/sum/',
	testFile = 'small.json';

var async = require('async'),
	floader = require('floader'),
	aloader = require('arrayloader');

floader.load(dataDirectory + testFile, function(err, config){

	var suite = JSON.parse(config);

	for(var i = 0; i < suite.length; i++){

		var prefix = String("0000" + (i + 1)).slice(-4);

		// directory containing matrix data files for current test
		var directory = dataDirectory + prefix + '/';

		var test = suite[i];

		var N = test.N;
		var M = 3;
		names = test.id.map(function(spec, i){ return "id_" + i;});

		var testName = "groupby.summulti: " + N + "x" + M + "x" + names.length;
		tape(testName, generateTestCase(directory, names, ["value_0"]));
	}
});

function generateTestCase(directory, id_names, value_names){
	return function(t){
		t.plan(1);

		var names = id_names.concat(value_names);
		// load columns from files
		load(directory, names, function(err, columns){

			floader.load(directory + "out.json", function(err, out){
				var expected = JSON.parse(out);

				var column_set = {};
				for (var i = 0; i < names.length; i++){
					var name = names[i];
					var column = columns[i];
					column_set[name] = column;
				}
				var frame = new Frame(column_set);

				var g = frame.groupbymulti(id_names);
				var actual = g.summulti(value_names[0]);

				t.equals(JSON.stringify(actual), JSON.stringify(expected));
			});

		});
	};
}

function loadInt32Array(path, cb){
	return aloader.load(path, Int32Array, cb);
}

function load(directory, names, callback){

	// array of paths to matrix data files for current test
	var paths = names.map(function(name){ return directory + name + ".i32";});

	//console.log(testFiles);
	async.map(paths, loadInt32Array,
		function(err, results){

			if(err) return callback(err);

			callback(err, results);
		}
	);
};

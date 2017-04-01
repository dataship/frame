var tape = require('tape'),
	Frame = require('../lib/frame');

// simple instructive test cases
function simpleTestCases(){

	tape("mean works with where", function(t){
		t.plan(1);
		var frame = new Frame({
			"id_0"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
			"id_1"  : [0, 0, 1, 1, 0, 0, 1, 0, 1],
			"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
		});

		var expected = 2.25; // 3 + 1 + 4 + 1

		var actual = frame.where("id_0", 1).mean("value");

		dtest.assert.close(t, actual, expected);
	});

	tape("mean works with where", function(t){
		t.plan(1);
		var frame = new Frame({
			"id_0"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
			"id_1"  : [0, 0, 1, 1, 0, 0, 1, 0, 1],
			"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
		});

		var expected = 2; // 1 + 2 + 2 + 3 + 2

		var actual = frame.where("id_0", 0).mean("value");

		dtest.assert.close(t, actual, expected);
	});

	tape("where does not modify mean on original Frame", function(t){
		t.plan(1);
		var frame = new Frame({
			"id_0"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
			"id_1"  : [0, 0, 1, 1, 0, 0, 1, 0, 1],
			"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
		});

		var expected = 2.1111111111; // (1 + 2 + 2 + 3 + 1 + 3 + 4 + 2 + 1) / 9

		var fw = frame.where("id_0", 0);
		var actual = frame.mean("value");

		dtest.assert.close(t, actual, expected);
	});

	tape("mean works with multiple wheres", function(t){
		t.plan(1);
		var frame = new Frame({
			"id_0"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
			"id_1"  : [0, 0, 1, 1, 0, 0, 1, 0, 1],
			"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
		});

		var expected = 2.666666666; // (3 + 4 + 1) / 3
		var actual = frame.where("id_0", 1).where("id_1", 1).mean("value");

		dtest.assert.close(t, actual, expected);
	});


	tape("mean works with where in", function(t){
		t.plan(1);
		var frame = new Frame({
			"id"  :   [0, 2, 0, 1, 1, 0, 2, 0, 1],
			"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
		});

		var expected = 2.3333333333333; // 1 + 2 + 2 + 3 + 4 + 2
		frame = frame.where("id", [0, 2]);
		var actual = frame.mean("value");

		dtest.assert.close(t, actual, expected);
	});
}

//simpleTestCases();

var RTOL = 1e-05, // 1e-05
	ATOL = 1e-12; // 1e-12

var dataDirectory = 'test/data/where.mean/',
	testFile = 'small.json';

var floader = require('floader'),
	dtest = require('../lib/test');

floader.load(dataDirectory + testFile, function(err, config){

	var suite = JSON.parse(config);
	simpleTestCases();

	for(var i = 0; i < suite.length; i++){

		var prefix = String("0000" + (i + 1)).slice(-4);

		// directory containing matrix data files for current test
		var directory = dataDirectory + prefix + '/';

		var test = suite[i];

		var names = test.id.map(function(spec, i){ return "id_" + i;});
		var types = test.id.map(function(spec, i){ return spec['type'];});

		var N = test.N; // number of rows
		var distincts = test.id.map(function(spec, i){ return spec.K; });

		var testName = "where.mean: " + N + " x " + "(" + distincts.join(", ") + ")"
		tape(testName, generateTestCase(directory, names, types, ["value_0"], [test.value[0].type]));
	}
});

var OUT_FILENAME = "out.json";


function generateTestCase(directory, id_names, id_types, value_names, value_types){
	return function(t){
		t.plan(1);

		var names = id_names.concat(value_names);
		var types = id_types.concat(value_types);

		// which columns require a key file?
		var key_names = id_names.filter(function(item, i){
			return id_types[i] in dtest.string_types
		});
		var key_types = id_types.filter(function(item, i){
			return item in dtest.string_types
		});

		// load columns from files
		dtest.load(directory, names, types, function(err, columns){

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

					var frame = new Frame(column_set, key_set);

					var value = column_set["id_0"][0];
					//console.log(subset);
					frame = frame.where("id_0", value);
					var actual = frame.mean("value_0");

					dtest.assert.close(t, actual, expected, "close", RTOL, ATOL);
				});

			});
		});
	};
}

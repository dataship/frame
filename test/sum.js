var tape = require('tape'),
	path = require('path'),
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

tape("groupbymulti.sum", function(t){
	t.plan(1);
	var frame = new Frame({
		"id_0"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"id_1"  : [0, 0, 1, 1, 0, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	var expected = {
		"0" : {
			"0" : 8,//[0, 1, 5, 7], 1 + 2 + 3 + 2
			"1" : 2//[2]
		},
		"1" : {
			"0" : 1,//[4],
			"1" : 8//[3, 6, 8] 3 + 4 + 1
		}
	};


	var g = frame.groupbymulti(["id_0", "id_1"]);
	var actual = g.summulti("value");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));
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

		var names = test.id.map(function(spec, i){ return "id_" + i;});
		var types = test.id.map(function(spec, i){ return spec['type'];});

		var N = test.N; // number of rows
		var distincts = test.id.map(function(spec, i){ return spec.K; });

		var testName = "groupby.summulti: " + N + " x " + "(" + distincts.join(", ") + ")"
		tape(testName, generateTestCase(directory, names, types, ["value_0"], [test.value[0].type]));
	}
});
var OUT_FILENAME = "out.json";
var DEFAULT_TYPE = "int32";

function generateTestCase(directory, id_names, id_types, value_names, value_types){
	return function(t){
		t.plan(1);

		var names = id_names.concat(value_names);
		var types = id_types.concat(value_types);
		// load columns from files
		load(directory, names, types, function(err, columns){

			floader.load(directory + OUT_FILENAME, function(err, out){
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

var type_map = {
	"int8" : ".i8",
	"uint8" : ".u8",
	"int16" : ".i16",
	"uint16" : ".u16",
	"int32" : ".i32",
	"uint32" : ".u32",
	"float32" : ".f32",
	"float64" : ".f64"
};

var extension_map = {
	".i8" : Int8Array,
	".u8" : Uint8Array,
	".i16" : Int16Array,
	".u16" : Uint16Array,
	".i32" : Int32Array,
	".u32" : Uint32Array,
	".f32" : Float32Array,
	".f64" : Float64Array
};
/* load a binary file as a TypedArray with the type given by the extension */
function loadArray(filePath, cb){

	var ext = path.extname(filePath);
	ext = ext.toLowerCase();

	if (ext in extension_map)
		constructor = extension_map[ext];
	else
		constructor = Int32Array;

	return aloader.load(filePath, constructor, cb);
}

function load(directory, names, types, callback){

	// array of paths to matrix data files for current test
	var paths = names.map(function(name, i){
		type = types[i]
		if (!(type in type_map)) type = DEFAULT_TYPE

		ext = type_map[types[i]]

		return directory + name + ext;
	});

	//console.log(testFiles);
	async.map(paths, loadArray,
		function(err, results){

			if(err) return callback(err);

			callback(err, results);
		}
	);
};

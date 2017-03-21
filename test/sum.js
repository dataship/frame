var tape = require('tape'),
	path = require('path'),
	Frame = require('../lib/frame');
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

var RTOL = 1e-05, // 1e-05
	ATOL = 1e-12; // 1e-12

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

				var asrt;
				if(value_types[0] in float_types){
					asrt = assert.treeclose;
				} else {
					asrt = assert.treeequal;
				}

				asrt(t, actual, expected, null, RTOL, ATOL);
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

var float_types = {
	"float32" : true,
	"float64" : true
}

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
}

function isobject(obj){ return Object.prototype.toString.call(obj) === "[object Object]";}

/* is there a key in object 'a' not found in object 'b'?
   if so, return the first key that's not found
   if not, return null
 */
function diffkeys(a, b){
	same_keys = true;
	for(key in a){
		same_keys &= (key in b);
		if(!same_keys){
			return key;
		}
	}

	return null;
}

/* comp is s comparison function for leaves
a - actual
b - expected
*/
function treediff(a, b, comp){

	var p = "<r>";
	var todo = [[a, b, p]]; // tuple of (a, b, p)
	var parents = { p : null };

	var diff_key = null,
		diff_a = null,
		diff_b = null;
	var t;
	while (todo.length > 0 && !diff_key){
		t = todo.pop();
		n_a = t[0];
		n_b = t[1];
		p = t[2];

		// are all the keys the same?
		diff_a = diffkeys(n_a, n_b);
		if(diff_a){
			diff_key = p;
			break;
		}
		diff_b = diffkeys(n_b, n_a);
		if(diff_b){
			diff_key = p;
			break;
		}

		// check children
		for(key in n_b){
			// both objects/internal nodes?
			if(isobject(n_b[key]) && isobject(n_a[key])){
				// yes, add to stack
				parents[key] = p;
				todo.push([n_a[key], n_b[key], key]);

			// both leaves?
			} else if(!isobject(n_b[key]) && !isobject(n_a[key])) {
				// yes, compare values
				if(!comp(n_b[key], n_a[key])){
					diff_key = key;
					diff_a = n_a[key];
					diff_b = n_b[key];
					break;
				}
			} else {
				// one is leaf the other is internal
				diff_key = key;
				if(is_object(n_b)){
					diff_a = n_a[key];
				} else {
					diff_b = n_b[key];
				}
				break;
			}
		}
	}

	var path;
	// difference found?
	if(diff_key){
		// yes, reconstruct the path
		var n = diff_key;
		path = [n];
		while(parents[n]){
			n = parents[n];
			path.push(n);
		}

		// diff_a and diff_b are both present on a leaf difference
		// only one is present for an internal node difference
		return {"path" : path.reverse(), "a" : diff_a, "b" : diff_b};
	}

	return null;

}

var assert = {};

/* determine whether two trees are equivalent
*/
assert.treeequal = function(t, a, b, msg) {
	var fail = treediff(a, b, function(a_n, b_n){
		return a_n === b_n;
	});

	msg = msg || 'should be treeequal';
	return assert.treeassert(t, fail, msg);
};

/* determine whether two trees are approximately equivalent:
internal nodes are identical
leaves are within specified floating point tolerances
 */
assert.treeclose = function(t, a, b, msg, RTOL, ATOL) {
	RTOL= RTOL || 1e-05;  // for 32 bit precision: 1e-06
	ATOL= ATOL || 1e-08;

	// treeequal with a floating point comparison function
	var fail = treediff(a, b, function(a_n, b_n){
		return Math.abs(a_n - b_n) <= ATOL + RTOL * Math.abs(b_n)
	});

	msg = msg || 'should be treeclose';
	return assert.treeassert(t, fail, msg);
};

assert.treeassert = function(t, fail, msg){

	if(fail){
		var actual = fail.path.join(" -> "),
			expected = fail.path.join(" -> ");

		if(fail.a){
			actual += " -> " + fail.a;
		}
		if(fail.b){
			expected += " -> " + fail.b;
		}
	}

	t._assert(!fail, {
		message : msg,
		operator : 'treeclose',
		actual : actual,
		expected : expected,
		extra : null
	});

	return !fail;
};

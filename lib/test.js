var async = require('async'),
	path = require('path'),
	floader = require('floader'),
	aloader = require('arrayloader');

test = {};

test.DEFAULT_TYPE = DEFAULT_TYPE = "int32";

test.type_map = type_map = {
	"int8" : ".i8",
	"uint8" : ".u8",
	"int16" : ".i16",
	"uint16" : ".u16",
	"int32" : ".i32",
	"uint32" : ".u32",
	"float32" : ".f32",
	"float64" : ".f64",
	"str8" : ".s8",
	"str16" : ".s16"
};

test.extension_map = extension_map = {
	".i8" : Int8Array,
	".u8" : Uint8Array,
	".i16" : Int16Array,
	".u16" : Uint16Array,
	".i32" : Int32Array,
	".u32" : Uint32Array,
	".f32" : Float32Array,
	".f64" : Float64Array,
	".s8" : Int8Array,
	".s16" : Int16Array
};

test.float_types = {
	"float32" : true,
	"float64" : true
};

test.string_types = {
	"str8" : true,
	"str16" : true
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

test.load = function(directory, names, types, callback){

	// array of paths to matrix data files for current test
	var paths = names.map(function(name, i){
		type = types[i];
		if (!(type in type_map)) type = DEFAULT_TYPE;

		ext = type_map[types[i]];

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
/* a key file is just a JSON array of strings
   the index of the string in the array is it's code
 */
function loadKey(filePath, cb){

	floader.load(filePath, function(err, key){
		if(err) return cb(err);

		return cb(null, JSON.parse(key));
	});
}

test.load_key = function(directory, names, types, callback){

	// array of paths to matrix data files for current test
	var paths = names.map(function(name, i){
		return directory + name + ".key";
	});

	//console.log(testFiles);
	async.map(paths, loadKey,
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

	var p = "(r)";
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
		diff_b = diffkeys(n_b, n_a);
		if(diff_b){
			diff_key = p;
			break;
		}
		diff_a = diffkeys(n_a, n_b);
		if(diff_a){
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

test.assert = {};
test.assert.tree = {};

/* determine whether two trees are equivalent
*/
test.assert.tree.equal = function(t, a, b, msg) {
	var fail = treediff(a, b, function(a_n, b_n){
		return a_n === b_n;
	});

	msg = msg || 'trees should be equal';
	return treeassert(t, fail, msg);
};

/* determine whether two trees are approximately equivalent:
internal nodes are identical
leaves are within specified floating point tolerances
 */
test.assert.tree.allclose = function(t, a, b, msg, RTOL, ATOL) {
	RTOL= RTOL || 1e-05;  // for 32 bit precision: 1e-06
	ATOL= ATOL || 1e-08;

	// treeequal with a floating point comparison function
	var fail = treediff(a, b, function(a_n, b_n){
		return Math.abs(a_n - b_n) <= ATOL + RTOL * Math.abs(b_n)
	});

	msg = msg || 'trees should be allclose';
	return treeassert(t, fail, msg);
};

var NULL_PLACEHOLDER = "(null)";
function treeassert(t, fail, msg){

	if(fail){
		var actual = fail.path.join(" -> "),
			expected = fail.path.join(" -> ");

		fail.a = fail.a || NULL_PLACEHOLDER;
		fail.b = fail.b || NULL_PLACEHOLDER;
		actual += " -> " + fail.a;
		expected += " -> " + fail.b;
	}

	t._assert(!fail, {
		message : msg,
		operator : 'tree.equal',
		actual : actual,
		expected : expected,
		extra : null
	});

	return !fail;
};

test.generate = {
	"Array" : {
		"int" : randomIntArray,
		"float" : randomFloatArray
	}
};

function randomIntArray(N, K){

	var data = [];

	for(var i = 0; i < N; i++){
		data.push(Math.random() * K | 0);
	}

	return data;
}

function randomFloatArray(N){

	var data = [];

	for(var i = 0; i < N; i++){
		data.push(Math.random() / Math.sqrt(N));
	}

	return data;
}

module.exports = test;

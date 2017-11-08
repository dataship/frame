
var reducers = require('./stream-reducers');
var BitArray = require('bit-array');


function isarray(obj){ return Object.prototype.toString.call(obj) === "[object Array]";}
function isobject(obj){ return Object.prototype.toString.call(obj) === "[object Object]";}
function isnumber(obj){ return Object.prototype.toString.call(obj) === "[object Number]";}
function isinteger(num){ return num % 1 === 0;}
function isstring(obj){ return Object.prototype.toString.call(obj) === "[object String]";}
function isfunction(obj){ return Object.prototype.toString.call(obj) === "[object Function]"; }
function isdate(obj){ return Object.prototype.toString.call(obj) === "[object Date]";}
var typed_array_constructors = {
	"[object Int32Array]" : true,
	"[object Uint32Array]" : true,
	"[object Float32Array]" : true,
	"[object Int8Array]" : true,
	"[object Uint8Array]" : true,
	"[object Int16Array]" : true,
	"[object Uint16Array]" : true,
	"[object Float64Array]" : true
}
function istypedarray(obj){
	var tag = Object.prototype.toString.call(obj);
	return tag in typed_array_constructors;
}


function shallowcopy(obj){
	if(obj == null) return obj; // null or undefined

	var copy = {};
	for(var key in obj){
		copy[key] = obj[key];
	}

	return copy;
}

//function isframe(obj){ return isarray(obj) && (obj.length == 0 || isobject(obj[0])); }


/* A lightweight, high performance Columnar Data Store disguised as a Data Frame
 *
 * Interface similarity targets and inspiration:
 * pandas, R, Linq, rethinkDB, Matlab
 *
 * column names:
 * columns.values.tolist(), colnames(f),
 *
 * aggregation:
 * groupby, , ,
 *
 * filtering:
 *
 * # References
 * https://github.com/StanfordHCI/datavore
 * http://vincentarelbundock.github.io/Rdatasets/datasets.html
 * https://galeascience.wordpress.com/2016/08/10/top-10-pandas-numpy-and-scipy-functions-on-github/
 * https://github.com/visualfabriq/bquery/blob/master/bquery/khash.h
 * ## R
 * http://www.r-tutor.com/r-introduction/data-frame
 * https://www.datacamp.com/community/tutorials/15-easy-solutions-data-frame-problems-r#gs.ArNaS44
 * ## Pandas
 * http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.groupby.html
 * http://chrisalbon.com/python/pandas_index_select_and_filter.html
 * ## Linq
 * https://msdn.microsoft.com/en-us/library/bb534304(v=vs.110).aspx?cs-save-lang=1&cs-lang=csharp#code-snippet-1
 */
/*	Create a data frame object from some data, like the Pandas and R objects
 *	of similar name.
 *
 *	@examples
 *
 *	// an array of row objects, like the output from babyparse and papaparse
 *
 *	rows =
	[
		{ "name" : "Finn",  "age" : 16, "title" : "Finn the Human"},
		{ "name" : "Jake", "age" : 32 , "title" : "Jake the Dog"},
		{ "name" : "Simon", "age" : 1043, "title" : "Ice King"},
		{ "name" : "Bonnibel", "age" : 827, "title" : "Princess Bubblegum"},
		{ "name" : "Marceline", "age" : 1004, "title" : "Marceline the Vampire Queen"}
	 ];
 *	df = Frame(rows);
 *
 * // an object (dict) mapping column names to arrays of values
 *
 * columns =
 * {
 *	"name" : ["Finn", "Jake", "Simon", "Bonnibel", "Marceline"],
 *	"age" : [16, 32, 1043, 827, 1004],
 *	"title" : ["Finn the Human", "Jake the Dog", "Ice King", "Princess Bubblegum", "Marceline the Vampire Queen"]
 * };
 *
 * df = Frame(columns);
 *
 * // an optional keys argument allows string columns to be more compactly
 * // represented when duplicates are present
 *
 *	columns =
 * {
 *	"name" : [0, 1, 2, 3, 4],
 *	"age" : [16, 32, 1043, 827, 1004],
 *	"title" : [0, 1, 2, 3, 4]
 * };
 *
 * keys = {
 * 	"name" : ["Finn", "Jake", "Simon", "Bonnibel", "Marceline"],
 *	"title" : ["Finn the Human", "Jake the Dog", "Ice King", "Princess Bubblegum", "Marceline the Vampire Queen"]
 * }
 *
 * df = Frame(columns, keys);
 *
 */
function Frame(data, keys, index, groups, filters){
	// f.constructor.name return "Frame"
	if(!(this instanceof Frame)) return new Frame(data, keys, index, groups, filters);

	if(Symbol && Symbol.toStringTag) this[Symbol.toStringTag] = 'Frame';


	// TODO: deep copy index
	if(index){
		Object.defineProperty(this, "_index", {
			"enumerable" : false,
			"value" : index
		});
	}

	// was a filters argument provided?
	if(filters){
		// yes, construct a single filter from the values
		var filter;
		for(key in filters){
			if(filter == null){
				filter = filters[key].copy();
			} else {
				filter.and(filters[key]);
			}
		}
		// copy of all defined filters
		Object.defineProperty(this, "_filters", {
			"enumerable" : false,
			"value" : filters
		});
		// single filter produced from combining all filters
		Object.defineProperty(this, "_filter", {
			"enumerable" : false,
			"value" : filter
		});
		Object.defineProperty(this, "_count", {
			"enumerable" : false,
			"value" : filter.count()
		});
	}
	if(groups){
		Object.defineProperty(this, "_groups", {
			"enumerable" : false,
			"value" : groups.slice(0)
		});
	}

	// do we have input?
	if(data == null){
		// no, just return an empty Frame
		return;
	}

	// what type of data input do we have?
	if(isobject(data)){
		// object, check it's values
		var column, length;

		for(var key in data){
			column = data[key];

			// are the items arrays?
			if(isarray(column) || istypedarray(column)){
				// yes, check for consistent lengths

				if(length == null){
					length = column.length;
				} else if(length !== column.length){
					throw new Error("Invalid data, arrays in object must be of equal length");
				}
			} else {
				// no, invalid data
				throw new Error("Invalid data, must be array of rows or dict of columns");
			}
		}

		Object.defineProperty(this, "length", {
			"enumerable" : false,
			"value" : length
		});

		// all checks pass use data as columns
		Object.defineProperty(this, "_cols", {
			"enumerable" : false,
			"value" : shallowcopy(data)
		});

		// do we also have a key/decoding object?
		if(keys && isobject(keys)){

			// check validity
			for(var key in keys){
				if(!(key in this._cols)) throw new Error("Invalid data, keys object doesn't match columns");
			}

			Object.defineProperty(this, "_keys", {
				"enumerable" : false,
				"value" : shallowcopy(keys)
			});
		}

	} else if(isarray(data)) {
		// array, check it's elements
		if(data.length == 0){
			return;
		}

		Object.defineProperty(this, "length", {
			"enumerable" : false,
			"value" : data.length
		});
		// all checks pass use data as columns
		Object.defineProperty(this, "_cols", {
			"enumerable" : false,
			"value" : {}
		});

		var row;
		for(key in data[0]){
			this._cols[key] = [];
		}
		for(var i = 0; i < data.length; i++){
			row = data[i];

			// are the rows objects?
			if(isobject(row)){
				// yes
				for(key in this._cols){
					if(key in row)
						this._cols[key][i] = row[key];
					else
						this._cols[key][i] = null;
				}
			} else {
				// no, invalid data
				throw new Error("Invalid data, must be array of rows or dict of columns");
			}
		}
	}

	// expose columns as properties
	for(name in this._cols){
		addColumn(this, name);
	}
}

Object.defineProperty(Frame.prototype, "add", {
	enumerable: false,
	value : function(name, values){

		if(this.length !== values.length)
			throw new Error("Invalid data, arrays in object must be of equal length");

		this._cols[name] = values;
		addColumn(this, name);
	}
});

// internal function for exposing a data column as a property on the Frame
function addColumn(frame, name){
	Object.defineProperty(frame, name, {
		enumerable : true,
		configurable: true,
		get: function(){
			// decode?
			var result = [];
			if(frame._keys && name in frame._keys){
				// yes, get keys
				var keys = frame._keys[name];

				// map data column onto decoded column
				// data column should be an array of indices into
				// the keys array
				var column = frame._cols[name];
				result = new Array(column.length);
				for(var i = 0; i < column.length;i++){
					result[i] = keys[column[i]];
				}
			} else {
				// no, just return the column
				result = frame._cols[name];
			}

			if(frame._filter){
				return result.filter(function(item, i){ return frame._filter.get(i);});
			} else {
				return result;
			}
		},
		set : function(data){
			if(!isarray(data)) throw new Error("data must be an array");
			if(data.length != frame.length) throw new Error("array must match length");

			if(frame._keys && name in frame._keys){
				throw new Error("setting keyed column not supported yet");
			} else {
				frame._cols[name] = data.slice(0);
			}
		}
	});
}

/*
// alternate syntax for toStringTag
get [Symbol.toStringTag]() {
	return 'Validator';
	}
*/
module.exports = Frame;

/*
	Get column names
 */
Object.defineProperty(Frame.prototype, "columns", {
	enumerable: false,
	get : function(){
		return Object.keys(this._cols);
	}
});

Object.defineProperty(Frame.prototype, "rename", {
	enumerable: false,
	value : function(old_name, new_name){
		if(!(old_name in this._cols))
			throw new Error("Couldn't find a column named '" + selector + "'");

		// copy column to new name
		var column = this._cols[old_name];
		this._cols[new_name] = column;

		// delete old column
		delete this._cols[old_name];
		delete this[old_name];

		// rename any decode key
		if(this._keys && old_name in this._keys){
			this._keys[new_name] = this._keys[old_name];
			delete this._keys[old_name]
		}

		addColumn(this, new_name);

	}
})

Object.defineProperty(Frame.prototype, "distinct", {"enumerable": false, "value" : distinct});

function distinct(selector){
	if(!(selector in this._cols))
		throw new Error("Couldn't find a column named '" + selector + "'");

	var key;
	if(this._keys) key = this._keys[selector];

	var column = this._cols[selector];
	var set = {};
	var value;
	for(var i = 0; i < column.length; i++){
		if(key) value = key[column[i]];
		else value = column[i];
		if(this._filter){
			if(this._filter.get(i)) set[value] = value;
		} else {
			set[value] = value;
		}
	}

	// this step enables non-string values
	var vals = [];
	for(key in set) vals.push(set[key]);

	return vals;
};

Object.defineProperty(Frame.prototype, "where", {"enumerable" : false, "value" : where});

/* element of, takes an array as an argument
 create and return a function that takes a single argument and returns true if
 that argument is contained in the given array

 NOTE: null and undefined may both be present in arr, and will be distinct from one another
*/
function el(arr){
	var set = {};
	for (var i = 0; i < arr.length; i++) set[arr[i]] = true;
	return function(v){ return set[v] != null;};
}

function eq(a){
	return function(v){ return v == a; };
}

function where(selector, condition){

	if(!(selector in this._cols))
		throw new Error("Couldn't find a column named '" + selector + "'");

	var column = this._cols[selector];
	var filter = new BitArray(this.length);

	var bits = filter.wordArray;
	var index = 0;
	var word = 0|0;
	var offset;
	var max = column.length - 1;

	if(isnumber(condition) || isstring(condition)){
		// keyed selector column?
		if(isstring(condition) && this._keys && selector in this._keys){
			// yes, encode condition
			var keys = this._keys[selector];
			condition = keys.indexOf(condition);
		}
		for(var i = 0; i < bits.length; i++){
			word = 0|0;
			offset = i * 32;
			var j = 31 + offset;
			if(j > max) j = max;
			for(; j >= offset; j--){
				if(column[j] === condition) word |= 1;
				if(j > offset) word <<= 1;
			}
			bits[i] = word;
		}
	} else {
		if(isarray(condition) || istypedarray(condition)){
			condition = el(condition);
		}
		if(this._keys && selector in this._keys){
			// yes, encode condition
			var keys = this._keys[selector];
		}

		var value;
		for(var i = 0; i < bits.length; i++){
			word = 0|0;
			offset = i * 32;
			var j = 31 + offset;
			if(j > max) j = max;
			for(; j >= offset; j--){
				if(keys) value = keys[column[j]];
				else value = column[j];
				if(condition(value)) word |= 1;
				if(j > offset) word <<= 1;
			}
			bits[i] = word;
		}
	}

	// create and return a new Frame with the new filter
	var filters = {};
	if(this._filters){
		Object.assign(filters, this._filters);
	}
	filters[selector] = filter;

	return new Frame(this._cols, this._keys, this._index, this._groups, filters);

}

Object.defineProperty(Frame.prototype, "join", {"enumerable" : false, "value" : join});
Object.defineProperty(Frame.prototype, "groupby", {"enumerable" : false, "value" : groupby});
Object.defineProperty(Frame.prototype, "ungroup", {"enumerable" : false, "value" : ungroup});
Object.defineProperty(Frame.prototype, "count", {"enumerable" : false, "value" : count});
Object.defineProperty(Frame.prototype, "argmax", {"enumerable" : false, "value": argmax});
Object.defineProperty(Frame.prototype, "argmin", {"enumerable" : false, "value": argmin});
Object.defineProperty(Frame.prototype, "min", {"enumerable" : false, "value": min});
Object.defineProperty(Frame.prototype, "max", {"enumerable" : false, "value": max});
Object.defineProperty(Frame.prototype, "sum", {"enumerable" : false, "value": sum});
Object.defineProperty(Frame.prototype, "mean", {"enumerable" : false, "value": mean});
Object.defineProperty(Frame.prototype, "median", {"enumerable" : false, "value": median});
Object.defineProperty(Frame.prototype, "reduce", {"enumerable" : false, "value": reduce});


/* use the partition method to find the median */
function median(selector){

	var column = this._cols[selector];
	var key = selector && this._keys ? this._keys[selector] : null;

	if (column.length == 0) return null;

	var p, m;

	middle = column.length / 2 | 0;

	var low = 0,
		high = column.length  - 1;

	var i = 0;
	// partition the array
	while(p != middle && i < column.length){
		i++;
		p = partition(column, low, high);

		if( p < middle) low = p + 1;
		else high = p - 1;
	}

	if(i == column.length){
		console.error("Maximum partition reached");
	}

	if(key) return key[column[p]];
	else return column[p];
}

/* partition an array, in place */
function partition(arr, low, high){

	if (low >= high) return high;

	// choose a random index for the pivot
	var pivot = randint(low, high);

	// swap pivot into last location
	swap(arr, high, pivot);

	pivot = low; // location of pivot in result
	// scan array and swap elements less than pivot into low end
	for(var i = low; i < high; i++){
		if (arr[i] < arr[high]){
			swap(arr, i, pivot);
			pivot++;
		}
	}

	swap(arr, high, pivot);

	return pivot;

}

/* get random integer in the inclusive interval [a, b]
	 a and b must be integers for correct performance
*/
function randint(a, b){
	r = Math.random(); //[0, 1)
	return a + Math.floor((b - a + 1)*r);
}

function swap(arr, i, j){
	var temp = arr[i];
	arr[i] = arr[j];
	arr[j] = temp;
}

function join(frame, link){

	// verify length of link column
	if(link.length !== this.length) throw new Error("Length of link column must match frame.");

	if(!("_cols" in frame)) throw new Error("First argument must be a frame.");

	// duplicate columns and keys
	var columns = shallowcopy(this._cols),
		keys = shallowcopy(this._keys) || {};

	// add virtual columns for each column in the joining frame
	for(name in frame._cols){
		// skip columns with duplicate names
		if(name in columns) continue;

		// don't join encoded columns
		if(frame._keys && name in frame._keys) continue;

		// add link column as encoded column data
		columns[name] = link;
		// add joining frame column as key column
		keys[name] = frame._cols[name];
	}

	return new Frame(columns, keys, this._index, this._groups, this._filters);

}


/*
 * group the data in the frame by a selector or set of selectors
 */
function groupby(){

	if(arguments.length == 0) throw new Error("No arguments provided");

	// collect arguments into list of selectors
	var selectors = [],
		arg;
	if(arguments.length === 1){
		arg = arguments[0];
		if(isstring(arg)) selectors = [arg];
		else if(isarray(arg)) selectors = arg;
	} else {
		for(var i = 0; i < arguments.length; i++){
			arg = arguments[i];
			if(!isstring(arg)) throw new Error("Invalid arguments");

			selectors.push(arg);
		}
	}

	var index = {};
	if(this._index){
		index = this._index;
		selectors = this._groups.concat(selectors);
	}

	// get references to all the columns involved in groups
	var columns = Array(selectors.length);
	var keys = {};
	for (var m = 0; m < selectors.length; m++){
		selector = selectors[m];

		if(!(selector in this._cols))
			throw new Error("Couldn't find a column named '" + selector + "'");

		columns[m] = this._cols[selector];
		if(this._keys && selector in this._keys) keys[m] = this._keys[selector];
	}

	var N = columns[0].length;
	var path = Array(columns.length);
	// iterate through rows
	for(var i = 0; i < N; i++){

		// compute distinct values for group columns describing the bin for
		// the current row
		for (var m = 0; m < columns.length; m++){
			var column = columns[m];
			if(m in keys) path[m] = keys[m][column[i]];
			else path[m] = column[i];
		}

		// add this row to the index using the group column values
		// by descending the hierarchy to the correct leaf
		var level = index;
		for(var j = 0; j < path.length - 1; j++){

			key = path[j];
			next = level[key];
			if(next == null || isarray(next)){
				next = {};
				level[key] = next;
			}
			level = next;
		}

		// update array of row indices stored in leaf
		key = path[path.length - 1];
		var arr = level[key];
		if(arr == null){
			level[key] = [i];
		} else {
			arr[arr.length] = i;
		}
	}

	/*
	this._index = index;
	this._groups = selectors.slice(0);
	return this;
	*/
	return new Frame(this._cols, this._keys, index, selectors, this._filters);
}

/* remove the grouping created by the last remaining groupby selector */
function ungroup(){
	if(this._index == null || this._groups.length < 1)
		throw new Error("Not enough groups")

	var frame = new Frame(this._cols, this._keys, null, null, this._filters);

	// handle special case of single group
	if(this._groups.length == 1)
		return frame;

	// for other cases do new groupby with one fewer groups
	return frame.groupby(this._groups.slice(0, -1));
}

function count(){
	if(this._index) return this.reduce();

	if(this._filter) return this._count;

	return this.length;
}

function min(selector){
	return this.reduce(selector, reducers.min);
}

function max(selector){
	return this.reduce(selector, reducers.max);
}

function sum(selector){
	return this.reduce(selector, reducers.sum);
}

function mean(selector){
	return this.reduce(selector, reducers.mean);
}

function argmax(selector){
	return this.reduce(selector, reducers.argmax);
}

function argmin(selector){
	return this.reduce(selector, reducers.argmin);
}

function reduce(selector, reducer, initial){

	var column = selector ? this._cols[selector] : null;
	var key = selector && this._keys ? this._keys[selector] : null;

	// choose default reduce, if none was supplied
	var is_numeric = column && column.length > 0 && Object.prototype.toString.call(column[0]) == "[object Number]";
	reducer = reducer || (is_numeric ? reducers.sum : reducers.max);

	if(this._index){
		return treereduce(column, key, this._index, this._keys, this._groups, this._filter, reducer, initial);
	} else if(this._filter) {
		return filterreduce(column, key, this._filter, reducer, initial);
	} else {
		return fullreduce(column, key, reducer, initial);
	}
}

function treereduce(column, rkey, index, keys, groups, filter, reducer, initial){

	var reduced = {};
	var parents = {};

	// depth first traversal
	var todo = [[index, null, 0]];
	var leaves = [];

	var result, pkey, level, n;
	while (todo.length > 0){
		n = todo.pop();// object
		index = n[0];
		pkey = n[1];
		level = n[2];
		result = {}; // container for this subtree in result

		var c, name;
		for(key in index){ // keys in object
			c = index[key];
			group = groups[level];

			// decode the key, if possible
			/*
			if(keys && group in keys){
				decoder = keys[group];
				key = decoder[key];
			}*/

			ckey = pkey ? pkey + "@" + key : key;

			if(isobject(c)){
				todo.push([c, ckey, level + 1]);
			} else {
				var indices = c;
				var filtered = filterindices(indices, filter);
				if(filtered.length != 0){
					var value;
					if(column){
						value = subsetreduce(column, rkey, filtered, reducer, initial);
					} else {
						value = filtered.length; // default to count
					}
					leaves.push([ckey, value]);
				}
			}
			parents[ckey] = [pkey, result];
		}
	}

	var root;
	while (leaves.length > 0){
		n = leaves.pop();
		ckey = n[0]; // composite key, parent + child
		value = n[1];

		p = parents[ckey];
		pkey = p[0];
		index = p[1];

		key = pkey ? ckey.slice(pkey.length + 1) : ckey;
		index[key] = value;
		if(pkey == null){
			root = index;
		} else {
			leaves.push([pkey, index]);
		}
	}

	return root;
};

function empty (obj){
	for (var key in obj) {
		if (obj.hasOwnProperty(key)) {
			return false
		}
	}
	return true
}

function filterindices(indices, filter){
	if(!filter) return indices;

	result = [];
	for(var i = 0; i < indices.length; i++){
		index = indices[i];
		if(filter.get(index)){
			result.push(index);
		}
	}
	return result;
}

/* reduce a subset of an array given by a set of indices using a supplied
   reducing function.

   Extracting this code into a function produces an order of magnitude speedup.
   I don't know why.
 */
function subsetreduce(column, key, indices, reducer, initial){

	var value = null;
	if(initial) value = initial;

	if(key){
		for(var i = 0; i < indices.length; i++){
			index = indices[i];
			if(value === null) value = key[column[index]];
			else value = reducer(value, key[column[index]], i);
		}
	} else {
		for(var i = 0; i < indices.length; i++){
			index = indices[i];
			if(value === null) value = column[index];
			else value = reducer(value, column[index], i);
		}
	}

	return value || 0;
}

function filterreduce(column, key, filter, reducer, initial){

	var value = null;
	if(initial) value = initial;

	var word,
		mask,
		cutoff;
	var bits = filter.wordArray;
	var total = 0;
	var max = column.length;

	if(key){
		for(var i = 0; i < bits.length; i++){
			word = bits[i];
			if(word !== 0){
				cutoff = (i + 1) * 32;
				if(cutoff > max) cutoff = max;
				mask = 1;
				for(var j = i * 32; j < cutoff; j++){
					if((word & mask) !== 0) {
						if(value === null) value = key[column[j]];
						else value = reducer(value, key[column[j]], total);
						total++;
					}
					mask <<= 1;
				}
			}
		}
	} else {
		for(var i = 0; i < bits.length; i++){
			word = bits[i];
			if(word !== 0){
				cutoff = (i + 1) * 32;
				if(cutoff > max) cutoff = max;
				mask = 1;
				for(var j = i * 32; j < cutoff; j++){
					if((word & mask) !== 0) {
						if(value === null) value = column[j];
						else value = reducer(value, column[j], total);
						total++;
					}
					mask <<= 1;
				}
			}
		}
	}


	return value || 0;
}

function fullreduce(column, key, reducer, initial){

	var start,
		value;

	// chose initial values and start of loop based on number of inputs and
	// supplied initial value
	if(initial !== void(0)){
		start = 0;
		value = initial;
	} else if(column.length > 0) {
		start = 1;
		value = key ? key[column[0]] : column[0];
	} else {
		start = 0;
		value = 0;
	}

	if(key){
		for(var i = start; i < column.length; i++){
			value = reducer(value, key[column[i]], i);
		}
	} else {
		for(var i = start; i < column.length; i++){
			value = reducer(value, column[i], i);
		}
	}

	return value;
}

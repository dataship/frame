
var FrameIndex = require('./frame-index.js');

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
 *	// an array of row objects, like the output from babyparse and papaparse
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
 *	// an object (dict) mapping column names to arrays of values
 *	columns =
	 {
		"name" : ["Finn", "Jake", "Simon", "Bonnibel", "Marceline"],
		"age" : [16, 32, 1043, 827, 1004],
		"title" : ["Finn the Human", "Jake the Dog", "Ice King", "Princess Bubblegum", "Marceline the Vampire Queen"]
	};
 *	df = Frame(columns);
 */
function Frame(data){
	// f.constructor.name return "Frame"
	if(!(this instanceof Frame)) return new Frame(data);

	this[Symbol.toStringTag] = 'Frame';

	Object.defineProperty(this, "_cols", {
		"enumerable" : false,
		"value" : {}
	});

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

		// all checks pass use data as columns
		for(var key in data){
			column = data[key];

			// copy column data
			// TODO: convert to TypedArrays here, if necessary and possible
			this._cols[key] = column.slice(0);
			//this._cols[key] = Array.from(column);
		}


	} else if(isarray(data)) {
		// array, check it's elements
		if(data.length == 0){
			return;
		}

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
// internal function for exposing a data column as a property on the Frame
function addColumn(frame, name){
	Object.defineProperty(frame, name, {
		enumerable : true,
		get: function(){
			return frame._cols[name];
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

/*
 * group the data in the frame by a selector or set of selectors
 */
Object.defineProperty(Frame.prototype, "groupby", {
	enumerable: false,
	value : function(selector){
		var index = {};

		if(!(selector in this._cols))
			throw new Error("Couldn't find a column named '" + selector + "'");

		var column = this._cols[selector],
			value,
			arr;

		for(var i = 0; i < column.length; i++){
			value = column[i];
			arr = index[value];
			if(arr !== undefined){
				arr[arr.length] = i;
			} else {
				index[value] = [i];
			}
		}

		return new FrameIndex(this, index);
	}
});


Object.defineProperty(Frame.prototype, "groupbymulti", {
	enumerable: false,
	value : function(selectors){
		var index = {};

		if (selectors.length == 0) return index;

		var columns = Array(selectors.length);
		for (var m = 0; m < selectors.length; m++){
			selector = selectors[m];

			if(!(selector in this._cols))
				throw new Error("Couldn't find a column named '" + selector + "'");

			columns[m] = this._cols[selector];
		}

		var N = columns[0].length;
		var path = Array(columns.length);
		for(var i = 0; i < N; i++){

			// compute distinct values for group columns describing the bin for current row
			for (var m = 0; m < columns.length; m++){
				var column = columns[m];
				path[m] = column[i];
			}

			// descend hierarchy of distinct group values to the correct leaf
			var level = index;
			for(var j = 0; j < path.length - 1; j++){

				key = path[j];
				next = level[key]
				if(next == null){
					next = {};
					level[key] = next;
				}
				level = next
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

		return new FrameIndex(this, index);
	}
});

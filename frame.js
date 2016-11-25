
var FrameIndex = require('./frame-index.js');

function isarray(obj){ return Object.prototype.toString.call(obj) === "[object Array]";}
function isobject(obj){ return Object.prototype.toString.call(obj) === "[object Object]";}
function isnumber(obj){ return Object.prototype.toString.call(obj) === "[object Number]";}
function isinteger(num){ return num % 1 === 0;}
function isstring(obj){ return Object.prototype.toString.call(obj) === "[object String]";}
function isfunction(obj){ return Object.prototype.toString.call(obj) === "[object Function]"; }
function isdate(obj){ return Object.prototype.toString.call(obj) === "[object Date]";}

//function isframe(obj){ return isarray(obj) && (obj.length == 0 || isobject(obj[0])); }


/* A lightweight, high performance Columnar Data Store disguised as a Data Frame
 *
 * Interface similarity targets and inspiration:
 * pandas, R, Linq, rethinkDB, Matlab
 *
 * aggregation:
 *	groupby, , ,
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

	this.cols = {};

	// do we have input?
	if(data == null){
		// no, just return an empty Frame
		this.cols = {};
		return;
	}

	// what type of data input do we have?
	if(isobject(data)){
		// object, check it's values
		var column, length;

		for(var key in data){
			column = data[key];

			// are the items arrays?
			if(isarray(column)){
				// yes, check their length

				if(length == null){
					length = column.length;
				} else if(length !== column.length){
					throw new Exception("Invalid data, arrays in object must be of equal length");
				}
			} else {
				// no, invalid data
				throw new Exception("Invalid data, must be array of rows or dict of columns");
			}
		}

		// all checks pass use data as columns
		for(var key in data){
			column = data[key];

			// copy column data
			this.cols[key] = column.slice(0);
		}


	} else if(isarray(data)) {
		// array, check it's elements
		if(data.length == 0){
			this.cols = {};
			return;
		}

		var row;
		var columns = {};
		for(key in data[0]){
			columns[key] = [];
		}
		for(var i = 0; i < data.length; i++){
			row = data[i];

			// are the rows objects?
			if(isobject(row)){
				// yes
				for(key in columns){
					if(key in row)
						columns[key].push(row[key])
					else
						columns[key].push(null);
				}
			} else {
				// no, invalid data
				throw new Exception("Invalid data, must be array of rows or dict of columns");
			}
		}
		this.cols = columns;
	}
}

module.exports = Frame;

Frame.prototype.labels = function(){
	return Object.keys(this.cols);
}

/*
 * group the data in the frame by a selector or set of selectors
 */
Frame.prototype.groupby = function(selector){
	var index = {};

	if(!(selector in this.cols))
		throw new Error("Couldn't find a column named '" + selector + "'");

	var column = this.cols[selector],
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

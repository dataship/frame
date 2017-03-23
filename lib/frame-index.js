
var reducers = require('./stream-reducers');

function isobject(obj){ return Object.prototype.toString.call(obj) === "[object Object]";}

/* A heirarchical index for the Frame data structure, the result of a call to
 * Frame.groupby
 */
function FrameIndex(frame, index, groups){
	this._frame = frame;
	this._index = index;
	this._groups = groups;
}

module.exports = FrameIndex;

/*
 */
FrameIndex.prototype.columns = function(){
	return this._frame.columns();
};

FrameIndex.prototype.groups = function(){
	return this._groups;
};

/* return a summary of the number of items in each group
 */
FrameIndex.prototype.count = function(){
	var counts = [];

	for(key in this._index){
		counts.push(this._index[key].length);
	}

	return counts;
};

FrameIndex.prototype.countmulti = function(){
	var reduced = {};
	var index = this._index;

	// depth first iteration
	var todo = [[index, reduced, 0]];

	var result;
	while (todo.length > 0){
		n = todo.pop();// object
		index = n[0];
		result = n[1];
		level = n[2];

		var c, name;
		for(key in index){ // keys in object
			c = index[key];
			name = this._groups[level];

			// decode the key, if possible
			if(this._frame._keys && name in this._frame._keys){
				decoder = this._frame._keys[name];
				key = decoder[key];
			}

			if(isobject(c)){
				result[key] = {};
				todo.push([c, result[key], level + 1]);
			} else {
				result[key] = c.length; // reduce
			}
		}
	}

	return reduced;

};

FrameIndex.prototype.sum = function(selector) {
	return this.reduce(selector, reducers.sum);
};

FrameIndex.prototype.summulti = function(selector){
	return this.reducemulti(selector, reducers.sum);
};

FrameIndex.prototype.reducemulti = function(selector, reducer, initial){

	var reduced = {};
	var index = this._index;
	var column = this._frame._cols[selector];

	reducer = reducer ||
		((column.length > 0 && Object.prototype.toString.call(column[0]) == "[object Number]") ?
			reducers.sum :
			reducers.max);

	// depth first traversal
	var todo = [[index, reduced, 0]];

	var result;
	while (todo.length > 0){
		n = todo.pop();// object
		index = n[0];
		result = n[1];
		level = n[2];

		var c, name;
		for(key in index){ // keys in object
			c = index[key];
			group = this._groups[level];

			// decode the key, if possible
			if(this._frame._keys && group in this._frame._keys){
				decoder = this._frame._keys[group];
				key = decoder[key];
			}

			if(isobject(c)){
				result[key] = {};
				todo.push([c, result[key], level + 1]);
			} else {
				var indices = c;
				var value = indexreduce(column, indices, reducer, initial);

				result[key] = value;
			}
		}
	}

	return reduced;

};

/* reduce a subset of an array given by a set of indices using a supplied
   reducing function.
 */
function indexreduce(column, indices, reducer, initial){

	var start,
		value;

	// chose initial values and start of loop based on number of inputs and
	// supplied initial value
	if(initial !== void(0)){
		start = 0;
		value = initial;
	} else if(indices.length > 0) {
		start = 1;
		value = column[indices[0]];
	} else {
		start = 0;
		value = 0;
	}

	for(var i = start; i < indices.length; i++){
		index = indices[i];
		value = reducer(value, column[index], i);
	}

	return value;

}

FrameIndex.prototype.reduce = function(selector, reducer, initial){
	if(!(selector in this._frame._cols))
		throw new Error("Couldn't find a column named '" + selector + "'");


	var column = this._frame._cols[selector],
		result = [],
		index,
		start;

	reducer = reducer ||
		((column.length > 0 && Object.prototype.toString.call(column[0]) == "[object Number]") ?
			reducers.sum :
			reducers.max);

	for(key in this._index){
		var indices = this._index[key];
		var value;

		value = indexreduce(column, indices, reducer, initial);

		result.push(value);
	}

	return result;
};

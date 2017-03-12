
var reducers = require('./stream-reducers');

function isobject(obj){ return Object.prototype.toString.call(obj) === "[object Object]";}

/* A heirarchical index for the Frame data structure, the result of a call to
 * Frame.groupby
 */
function FrameIndex(frame, index){
	this.frame = frame;
	this.index = index;
}

module.exports = FrameIndex;

/*
 */
FrameIndex.prototype.columns = function(){
	return this.frame.columns();
};

FrameIndex.prototype.groups = function(){
	return Object.keys(this.index);
};

/* return a summary of the number of items in each group
 */
FrameIndex.prototype.count = function(){
	var counts = [];

	for(key in this.index){
		counts.push(this.index[key].length);
	}

	return counts;
};

FrameIndex.prototype.countmulti = function(){
	var reduced = {};
	var index = this.index;

	// depth first iteration
	var todo = [[index, reduced]];

	var result;
	while (todo.length > 0){
		n = todo.pop();// object
		index = n[0];
		result = n[1];

		var c;
		for(key in index){ // keys in object
			c = index[key];

			if(isobject(c)){
				result[key] = {};
				todo.push([c, result[key]]);
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
	var index = this.index;
	var column = this.frame._cols[selector];

	reducer = reducer ||
		((column.length > 0 && Object.prototype.toString.call(column[0]) == "[object Number]") ?
			reducers.sum :
			reducers.max);

	// depth first traversal
	var todo = [[index, reduced]];

	var result;
	while (todo.length > 0){
		n = todo.pop();// object
		index = n[0];
		result = n[1];

		var c;
		for(key in index){ // keys in object
			c = index[key];

			if(isobject(c)){
				result[key] = {};
				todo.push([c, result[key]]);
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
function indexreduce(arr, indices, reducer, initial){

	var start,
		value;

	// chose initial values and start of loop based on number of inputs and
	// supplied initial value
	if(initial !== void(0)){
		start = 0;
		value = initial;
	} else if(indices.length > 0) {
		start = 1;
		value = arr[indices[0]];
	} else {
		start = 0;
		value = 0;
	}

	for(var i = start; i < indices.length; i++){
		index = indices[i];
		value = reducer(value, arr[index], i);
	}

	return value;

}

FrameIndex.prototype.reduce = function(selector, reducer, initial){
	if(!(selector in this.frame._cols))
		throw new Error("Couldn't find a column named '" + selector + "'");


	var column = this.frame._cols[selector],
		result = [],
		index,
		start;

	reducer = reducer ||
		((column.length > 0 && Object.prototype.toString.call(column[0]) == "[object Number]") ?
			reducers.sum :
			reducers.max);

	for(key in this.index){
		var indices = this.index[key];
		var value;

		value = indexreduce(column, indices, reducer, initial);

		result.push(value);
	}

	return result;
};

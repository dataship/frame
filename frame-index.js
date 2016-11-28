
var reducers = require('./stream-reducers');

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
FrameIndex.prototype.labels = function(){
	return Object.keys(this.index);
}

/* return a summary of the number of items in each group
 */
FrameIndex.prototype.count = function(){
	var counts = [];

	for(key in this.index){
		counts.push(this.index[key].length);
	}

	return counts;
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
		var indeces = this.index[key];
		var value;

		if(initial !== void(0)){
			start = 0;
			value = initial;
		} else if(indeces.length > 0) {
			start = 1;
			value = column[indeces[0]];
		} else {
			start = 0;
			value = 0;
		}

		for(var i = start; i < indeces.length; i++){
			index = indeces[i];
			value = reducer(value, column[index], i);
		}
		result.push(value);
	}

	return result;
}

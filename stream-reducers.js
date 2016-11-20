
module.exports = {
	"count" : count,
	"sum" : sum,
	"max" : max,
	"min" : min,
	"mean" : mean,
	"mode" : mode,
	"median" : median
};

/* Array.prototype.reduce style function for finding the maximum
 * @examples
 * [1, 1, 1].reduce(ds.reduce.max);			// => 1
 * [3, 1, 3, 5].reduce(ds.reduce.max);		// => 5
 * reduce({"a" : 1, "b" : 0, "c" : 2}, ds.reduce.max);	// => 2
 */
function max(agg, val) { return agg > val ? agg : val; };

/* Array.prototype.reduce style function for finding the minimum
 * @examples
 * [1, 1, 1].reduce(ds.reduce.min);			// => 1
 * [3, 1, 3, 5].reduce(ds.reduce.min);		// => 1
 * reduce({"a" : 1, "b" : 0, "c" : 2}, ds.reduce.min);	// => 0
 */
function min(agg, val) { return agg < val ? agg : val; };

/* Array.prototype.reduce style function for finding the most common value
 * @examples
 * [1, 1, 1].reduce(ds.reduce.mode);			// => 1
 * [1, 3, 3, 7].reduce(ds.reduce.mode);		// => 3
 * reduce({"a" : 1, "b" : 0, "c" : 2}, ds.reduce.mode);	// => 1
 */
function mode(agg, val, n) {
	if(n === 0) return val;

	if(n === 1){
		// internal state hack (compatible with groupby)
		self = mode.state = {};
		self.values = {};
		self.values[agg] = 1;
		self.argmax = agg;
	} else {
		self = mode.state;
	}

	if(val in self.values)
		self.values[val] += 1;
	else
		self.values[val] = 1;

	if(self.values[val] > self.values[agg])
		self.argmax = val;

	return self.argmax;
}

/* Array.prototype.reduce style function for finding the middle value
 * @examples
 * [1, 1, 1].reduce(ds.reduce.median);			// => 1
 * [1, 3, 3, 7].reduce(ds.reduce.median);		// => 3
 * [4, 1, 7].reduce(ds.reduce.median);			// => 4
 * reduce({"a" : 4, "b" : 1, "c" : 7}, ds.reduce.median);	// => 4
 */
function median(agg, val, n) {
	if(n === 0) return val;

	if(n === 1){
		// internal state hack (compatible with groupby)
		self = median.state = {};
		self.values = [agg];
	} else {
		self = median.state;
	}

	// insert the new value into the sorted array
	insert(self.values, val);

	var middle = self.values.length / 2 | 0;
	// even number of elements?
	if(self.values.length % 2 !== 0){
		// no, return the middle one
		return self.values[middle];
	} else {
		// yes, return the average of the middle two
		return (self.values[middle - 1] + self.values[middle]) / 2;
	}
}

/* Array.prototype.reduce style function for counting number of elements
 * @examples
 * [1, 1, 1].reduce(ds.reduce.count);			// => 3
 * [3, 1, 3, 5].reduce(ds.reduce.count);		// => 4
 * reduce({"a" : 1, "b" : 0, "c" : 2}, ds.reduce.count);	// => 3
 */
function count(agg, val, n){ return n + 1; };

/* Array.prototype.reduce style function for finding the sum
 * @examples
 * [1, 1, 1].reduce(ds.reduce.sum);			// => 3
 * [3, 1, 3, 5].reduce(ds.reduce.sum);		// => 12
 * reduce({"a" : 1, "b" : 0, "c" : 2}, ds.reduce.sum);	// => 3
 */
function sum(agg, val){ return agg + val; };

/* Array.prototype.reduce style function for finding the arithmetic mean
 * @examples
 * [1, 1, 1].reduce(ds.reduce.mean);			// => 1
 * [3, 1, 3, 5].reduce(ds.reduce.mean);		// => 3
 * reduce({"a" : 1, "b" : 0, "c" : 2}, ds.reduce.mean);	// => 1
 */
function mean(agg, val, n){ return (agg + ((val - agg)/(n + 1))); };

var d = function(a, b){ return a > b ? 1 : a < b ? -1 : 0;};

function insert(arr, el){
	var index = binarySearch(arr, el, d);
	arr.splice(index, 0, el);

	return arr;
};

var binarySearch = function binarySearch(arr, el, comparator) {

	var m = 0;
	var n = arr.length - 1;
	while (m <= n) {
		var k = (n + m) >> 1;
		var cmp = comparator(el, arr[k]); // comparator(arr[k], el);
		if (cmp > 0) {
			m = k + 1;
		} else if(cmp < 0) {
			n = k - 1;
		} else {
			return k;
		}
	}

	return m;
}

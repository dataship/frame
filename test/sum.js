var test = require('tape'),
	Frame = require('../frame');

test("groupby.sum", function(t){
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

test("groupby.sum.strings", function(t){
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

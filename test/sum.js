var test = require('tape'),
	Frame = require('../frame');

test("groupby.sum", function(t){
	t.plan(1);
	var frame = new Frame({
		"group"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"reduce" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	var expected = [10, 9];

	var g = frame.groupby("group");
	var actual = g.reduce("reduce");

	t.equals(JSON.stringify(actual), JSON.stringify(expected), "reduce");

	/*
	t.pass("this is how we do it.");
	t.comment("extra stuff: 3.1415");
	*/
});

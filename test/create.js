var test = require('tape'),
	Frame = require('../frame');

test("access column from hidden property", function(t){
	t.plan(1);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];

	var frame = new Frame({
		"a" : a,
		"b" : b
	});


	t.equals(JSON.stringify(frame._cols["a"]), JSON.stringify(a));
});

test("access column as property", function(t){
	t.plan(1);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];

	var frame = new Frame({
		"a" : a,
		"b" : b
	});


	t.equals(JSON.stringify(frame["a"]), JSON.stringify(a));
});

test("only columns are enumerable", function(t){
	t.plan(1);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];

	var frame = new Frame({
		"a" : a,
		"b" : b
	});


	t.equals(JSON.stringify(Object.keys(frame)), JSON.stringify(["a", "b"]));
});

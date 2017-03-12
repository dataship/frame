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

test("row based constructor creates columns correctly", function(t){
	t.plan(2);
	var rows = [
		{"a" : 0, "b" : 1},
		{"a" : 0, "b" : 2},
		{"a" : 0, "b" : 2},
		{"a" : 1, "b" : 3},
		{"a" : 1, "b" : 1},
		{"a" : 0, "b" : 3},
		{"a" : 1, "b" : 4},
		{"a" : 0, "b" : 2},
		{"a" : 1, "b" : 1},
	];

	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];

	var frame = new Frame(rows);


	t.equals(JSON.stringify(frame._cols["a"]), JSON.stringify(a));
	t.equals(JSON.stringify(frame._cols["b"]), JSON.stringify(b));
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
	t.plan(2);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];

	var frame = new Frame({
		"a" : a,
		"b" : b
	});

	var expected = ["a", "b"];

	t.equals(JSON.stringify(Object.keys(frame)), JSON.stringify(expected));

	var found = [];

	for(name in frame){
		found.push(name);
	}

	t.equals(JSON.stringify(found), JSON.stringify(expected));
});

test("Symbol.toStringTag correctly overridden", function(t){
	t.plan(1);
	var frame = new Frame({
		"a" : [0],
		"b" : [1]
	});

	var expected = "[object Frame]";

	t.equals(Object.prototype.toString.call(frame), expected);
});

var test = require('tape'),
	Frame = require('../lib/frame');

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

test("access keys from hidden property", function(t){
	t.plan(1);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];
	var k = ["one", "two"];

	var frame = new Frame({
			"a" : a,
			"b" : b
		},
		{
			"a" : k
	});


	t.equals(JSON.stringify(frame._keys["a"]), JSON.stringify(k));
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

test("accessing column as property decodes when key is present", function(t){
	t.plan(1);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];
	var k = ["one", "two"];

	var frame = new Frame({
			"a" : a,
			"b" : b
		},
		{
			"a" : k
	});


	var expected = ["one", "one", "one", "two", "two", "one", "two", "one", "two"];
	t.equals(JSON.stringify(frame["a"]), JSON.stringify(expected));
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

test("rename column correctly modifies frame properties", function(t){
	t.plan(2);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];

	var frame = new Frame({
		"a" : a,
		"b" : b
	});

	var expected = ["a", "c"];

	frame.rename("b", "c");

	t.equals(JSON.stringify(Object.keys(frame)), JSON.stringify(expected));

	var found = [];

	for(name in frame){
		found.push(name);
	}

	t.equals(JSON.stringify(found), JSON.stringify(expected));
});

test("rename column correctly adds accessor", function(t){
	t.plan(1);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];

	var frame = new Frame({
		"a" : a,
		"b" : b
	});

	var expected = b;

	frame.rename("b", "c");

	t.equals(JSON.stringify(frame["c"]), JSON.stringify(expected));
});

test("rename column correctly converts key", function(t){
	t.plan(1);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];

	var frame = new Frame({
		"a" : a,
		"b" : b
	},
	{
		"b" : ["zero", "one", "two", "three", "four"]
	});

	var expected = ["one", "two", "two", "three", "one", "three", "four", "two", "one"];

	frame.rename("b", "c");

	t.equals(JSON.stringify(frame["c"]), JSON.stringify(expected));
});

test("setting via property accessor works correctly", function(t){
	t.plan(1);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];

	var frame = new Frame({
		"a" : a,
		"b" : b
	});
	var c = [3, 4, 1, 0, 2, 1, 2, 3, 3];

	frame["b"] = c;

	var expected = c.slice(0);
	t.equals(JSON.stringify(frame["b"]), JSON.stringify(expected));
});

test("distinct works correctly", function(t){
	t.plan(2);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];

	var frame = new Frame({
		"a" : a,
		"b" : b
	});

	var expected = [1, 2, 3, 4];
	var actual = frame.distinct("b");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));

	var expected = [0, 1];
	var actual = frame.distinct("a");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));
});

test("distinct works with keyed column", function(t){
	t.plan(1);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];

	var frame = new Frame({
		"a" : a,
		"b" : b
	}, {
		"a" : ["zero", "one"]
	});

	var expected = ["zero", "one"];
	var actual = frame.distinct("a");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));
});

test("distinct works with where", function(t){
	t.plan(2);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];

	var frame = new Frame({
		"a" : a,
		"b" : b
	});

	var expected = [1, 3, 4];
	frame = frame.where("a", 1);
	var actual = frame.distinct("b");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));

	var expected = [1];
	var actual = frame.distinct("a");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));
});

test("min works with where", function(t){
	t.plan(2);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 4, 3, 4, 2, 3];

	var frame = new Frame({
		"a" : a,
		"b" : b
	});

	var expected = 3;
	frame = frame.where("a", 1);
	var actual = frame.min("b");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));

	var expected = 1;
	var actual = frame.min("a");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));
});

test("min works correctly on ISO date strings", function(t){
	t.plan(1);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = ["2016-03-11", "2016-05-11", "2016-04-10", "2016-03-15",
			 "2016-03-03", "2016-04-21", "2016-05-28", "2016-03-17",
			 "2016-04-04"];

	var frame = new Frame({
		"a" : a,
		"b" : b
	});

	var expected = "2016-03-03";
	var actual = frame.min("b");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));
});

test("min works correctly on keyed column", function(t){
	t.plan(1);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];
	k = ["b", "a"];

	var frame = new Frame({
		"a" : a,
		"b" : b
	}, {
		"a" : k
	});

	var expected = "a";
	var actual = frame.min("a");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));
});

test("max works correctly", function(t){
	t.plan(2);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];

	var frame = new Frame({
		"a" : a,
		"b" : b
	});

	var expected = 4;
	var actual = frame.max("b");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));

	var expected = 1;
	var actual = frame.max("a");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));
});

test("max works correctly on ISO date strings", function(t){
	t.plan(1);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = ["2016-03-11", "2016-05-11", "2016-04-10", "2016-03-15",
			 "2016-03-03", "2016-04-21", "2016-05-28", "2016-03-17",
			 "2016-04-04"];

	var frame = new Frame({
		"a" : a,
		"b" : b
	});

	var expected = "2016-05-28";
	var actual = frame.max("b");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));
});

test("max works correctly on keyed column", function(t){
	t.plan(1);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];
	k = ["b", "a"];

	var frame = new Frame({
		"a" : a,
		"b" : b
	}, {
		"a" : k
	});

	var expected = "b";
	var actual = frame.max("a");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));
});

test("add creates new column", function(t){
	t.plan(3);
	var a = [0, 0, 0, 1, 1, 0, 1, 0, 1];
	var b = [1, 2, 2, 3, 1, 3, 4, 2, 1];
	var c = [2, 7, 2, 1, 9, 3, 2, 1, 1];

	var frame = new Frame({
		"a" : a,
		"b" : b
	});

	var expected = ["a", "b", "c"];

	frame.add("c", c);

	t.equals(JSON.stringify(Object.keys(frame)), JSON.stringify(expected));

	var found = [];

	for(name in frame){
		found.push(name);
	}

	t.equals(JSON.stringify(found), JSON.stringify(expected));

	t.equals(JSON.stringify(frame["c"]), JSON.stringify(c));
});

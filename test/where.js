var tape = require('tape'),
	BitArray = require('bit-array'),
	Frame = require('../lib/frame');

tape("where creates correct filter", function(t){
	t.plan(1);

	var frame = new Frame({
		"id"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	//frame.where(row => row.id == 1);
	frame = frame.where("id", v => v == 1);

	var expected = new BitArray(9);

	expected.set(3, true);
	expected.set(4, true);
	expected.set(6, true);
	expected.set(8, true);

	var actual = frame._filter;
	t.equals(actual.toString(), expected.toString());
});

tape("where with numerical argument creates correct filter", function(t){
	t.plan(1);

	var frame = new Frame({
		"id"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	frame = frame.where("id", 1);

	var expected = new BitArray(9);

	expected.set(3, true);
	expected.set(4, true);
	expected.set(6, true);
	expected.set(8, true);

	var actual = frame._filter;
	t.equals(actual.toString(), expected.toString());
});

tape("where with array argument creates correct filter", function(t){
	t.plan(1);

	var frame = new Frame({
		"id"  : [0, 2, 0, 1, 1, 0, 2, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	frame = frame.where("id", [0, 2]);

	var expected = new BitArray(9);

	expected.set(0, true);
	expected.set(1, true);
	expected.set(2, true);
	expected.set(5, true);
	expected.set(6, true);
	expected.set(7, true);

	var actual = frame._filter;
	t.equals(actual.toString(), expected.toString());
});

tape("where with virtual column creates correct filter", function(t){
	t.plan(1);

	var frame = new Frame({
		"id"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	frame = frame.where(row => row.id == 1);

	var expected = new BitArray(9);

	expected.set(3, true);
	expected.set(4, true);
	expected.set(6, true);
	expected.set(8, true);

	var actual = frame._filter;
	t.equals(actual.toString(), expected.toString());
});

tape("where creates second filter correctly", function(t){
	t.plan(1);

	var frame = new Frame({
		"id_0"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"id_1"  : [0, 0, 1, 1, 0, 1, 0, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	//frame.where(row => row.id == 1);
	frame = frame.where("id_1", id => id == 1);
	frame = frame.where("id_0", id => id == 1);

	var expected = new BitArray(9);

	expected.set(3, true);
	expected.set(8, true);

	var actual = frame._filter;
	t.equals(actual.toString(), expected.toString());
});

tape("where filters column via accessor", function(t){
	t.plan(1);

	var frame = new Frame({
		"id"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	//frame.where(row => row.id == 1);
	frame = frame.where("id", v => v == 1);

	var expected = [3, 1, 4, 1];


	var actual = frame["value"];
	t.equals(actual.toString(), expected.toString());
});

tape("where filters keyed column via accessor", function(t){
	t.plan(1);

	var columns = {
		"id"  :   [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [6, 1, 5, 3, 1, 2, 4, 0, 1]
	};
	var keys = {
		"value" : ["fare", "fish", "my", "red", "blue", "to", "add"]
	};

	var frame = new Frame(columns, keys);

	frame = frame.where("id", v => v == 1);

	var expected = ["red", "fish", "blue", "fish"];


	var actual = frame["value"];
	t.equals(actual.toString(), expected.toString());
});

tape("where accepts string filter on keyed column", function(t){
	t.plan(1);

	var columns = {
		"id"  :   [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [6, 1, 5, 3, 1, 2, 4, 0, 1]
	};
	var keys = {
		"id" : ["thoreau", "seuss"],
		"value" : ["fare", "fish", "my", "red", "blue", "to", "add"]
	};

	var frame = new Frame(columns, keys);

	frame = frame.where("id", "thoreau");

	var expected = ["add", "fish", "to", "my", "fare"];


	var actual = frame["value"];
	t.equals(actual.toString(), expected.toString());
});

tape("where accepts function with string on keyed column", function(t){
	t.plan(1);

	var columns = {
		"id"  :   [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [6, 1, 5, 3, 1, 2, 4, 0, 1]
	};
	var keys = {
		"id" : ["thoreau", "seuss"],
		"value" : ["fare", "fish", "my", "red", "blue", "to", "add"]
	};

	var frame = new Frame(columns, keys);

	frame = frame.where("id", v => v == "seuss");

	var expected = ["red", "fish", "blue", "fish"];

	var actual = frame["value"];
	t.equals(actual.toString(), expected.toString());
});

tape("where filter can be modified", function(t){
	t.plan(2);

	var columns = {
		"id"  :   [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [6, 1, 5, 3, 1, 2, 4, 0, 1]
	};
	var keys = {
		"id" : ["thoreau", "seuss"],
		"value" : ["fare", "fish", "my", "red", "blue", "to", "add"]
	};

	var frame = new Frame(columns, keys);


	frame = frame.where("id", "thoreau");
	var expected = ["add", "fish", "to", "my", "fare"];

	var actual = frame["value"];
	t.equals(actual.toString(), expected.toString());

	frame = frame.where("id", v => v == "seuss");
	var expected = ["red", "fish", "blue", "fish"];

	var actual = frame["value"];
	t.equals(actual.toString(), expected.toString());
});
/*
function eq(a){
	return function(v){ v == a; };
}

function in(arr){
	var set = {};
	for (a in arr) set[a] = true;
	return function(v){ return v in set;};
}*/

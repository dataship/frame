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

	frame.where("id", 1);

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

	frame.where("id", [0, 2]);

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


/*
function eq(a){
	return function(v){ v == a; };
}

function in(arr){
	var set = {};
	for (a in arr) set[a] = true;
	return function(v){ return v in set;};
}*/

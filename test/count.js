var tape = require('tape'),
	Frame = require('../lib/frame');

tape("count gives length with no filter", function(t){
	t.plan(1);

	var frame = new Frame({
		"id"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	var expected = 9;

	var actual = frame.count();
	t.equals(actual, expected);
});

tape("count works with where", function(t){
	t.plan(1);

	var frame = new Frame({
		"id"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	//frame.where(row => row.id == 1);
	frame = frame.where("id", v => v == 1);

	var expected = 4;

	var actual = frame.count();
	t.equals(actual, expected);
});

tape("count works with where.equals", function(t){
	t.plan(1);

	var frame = new Frame({
		"id"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	frame = frame.where("id", 1);

	var expected = 4;

	var actual = frame.count();
	t.equals(actual, expected);
});

tape("count works with where.in", function(t){
	t.plan(1);

	var frame = new Frame({
		"id"  : [0, 2, 0, 1, 1, 0, 2, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	frame = frame.where("id", [0, 2]);

	var expected = 6;

	var actual = frame.count();
	t.equals(actual, expected);
});

tape("count works with multiple where", function(t){
	t.plan(1);

	var frame = new Frame({
		"id_0"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"id_1"  : [0, 0, 1, 1, 0, 1, 0, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	//frame.where(row => row.id == 1);
	frame = frame.where("id_1", id => id == 1);
	frame = frame.where("id_0", id => id == 1);

	var expected = 2;

	var actual = frame.count();
	t.equals(actual, expected);
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

var tape = require('tape'),
	Frame = require('../frame');
/*
	ports = frame.groupby("ports");
	for (name in ports){

		port = ports[name];

		// three possibilities
		port.sum("deficiencies");
		port.deficiencies.sum();
		port["deficiencies"].sum();

	}


	ports_types = frame.groupby("ports", "types");
	for (port_name in ports_types){
		types = ports_types[port_name];

		for(type_name in types){
			type = types[type_name];

			type.deficiencies.sum();

		}
	}
 */
/*

result = groupby("country")
result.levels() // => 1 (tiers, dimensions)

result.sum("deficiencies", "detained"); // => [[6, 5, 10, 7], [2, 0, 1, 2]]

result.china.sum("deficiencies", "detained"); // => [6, 2]
result.china.sum("deficiencies"); // => 6

result.china.values("deficiencies") // => [1, 1, 0, 1, 1, 0, 1, 0, 1]

result.groups() // => ["china", "brazil", "new zealand", "korea"]

result.china.groups() // => ["oil tanker", "cargo ship"]


ports = f.groupby("port")

ports.values("country") // => [["china", "china", "china"], ["brazil", "brazil"], "new zealand", "korea"]

ports.distinct("country") // => ["china", "brazil", "new zealand", "korea"]

country_type = inspections.groupby("country", "type")

// pivot table, (generalized contingency table/cross tabulation (non-frequency))
pivot = country_type.sum("deficiencies") // => [[1, 4, 5, 6], [], [], [], [], ]

domain_category = pins.groupby("domain", "category")

// pivot table, cross tabulation
domain_category.count()
*/
/*
test("groupby.count", function(t){
	t.plan(1);
	var frame = new Frame({
		"id"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	var expected = [5, 4];

	var g = frame.groupby("id");
	var actual = g.count();

	t.equals(JSON.stringify(actual), JSON.stringify(expected));

});

test("groupby.sum", function(t){
	t.plan(1);
	var frame = new Frame({
		"id"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	var expected = [10, 9];

	var g = frame.groupby("id");
	var actual = g.sum("value");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));

});

test("groupbymulti", function(t){
	t.plan(1);
	var frame = new Frame({
		"id_0"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"id_1"  : [0, 0, 1, 1, 0, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	var expected = {
		"0" : {
			"0" : [0, 1, 5, 7],
			"1" : [2]
		},
		"1" : {
			"0" : [4],
			"1" : [3, 6, 8]
		}
	};


	var g = frame.groupbymulti(["id_0", "id_1"]);

	t.equals(JSON.stringify(g.index), JSON.stringify(expected));
});

test("groupbymulti.count", function(t){
	t.plan(1);
	var frame = new Frame({
		"id_0"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"id_1"  : [0, 0, 1, 1, 0, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	var expected = {
		"0" : {
			"0" : 4,
			"1" : 1
		},
		"1" : {
			"0" : 1,
			"1" : 3
		}
	};


	var g = frame.groupbymulti(["id_0", "id_1"]);
	var actual = g.countmulti();

	t.equals(JSON.stringify(actual), JSON.stringify(expected));
});

test("groupbymulti.sum", function(t){
	t.plan(1);
	var frame = new Frame({
		"id_0"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"id_1"  : [0, 0, 1, 1, 0, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	var expected = {
		"0" : {
			"0" : 8,//[0, 1, 5, 7], 1 + 2 + 3 + 2
			"1" : 2//[2]
		},
		"1" : {
			"0" : 1,//[4],
			"1" : 8//[3, 6, 8] 3 + 4 + 1
		}
	};


	var g = frame.groupbymulti(["id_0", "id_1"]);
	var actual = g.summulti("value");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));
});
*/

var RTOL = 1e-05,
	ATOL = 1e-12;

var dataDirectory = 'test/data/count/',
	testFile = 'small.json';

var async = require('async'),
	floader = require('floader'),
	aloader = require('arrayloader');

floader.load(dataDirectory + testFile, function(err, config){

	var suite = JSON.parse(config);

	for(var i = 0; i < suite.length; i++){

		var prefix = String("0000" + (i + 1)).slice(-4);

		// directory containing matrix data files for current test
		var directory = dataDirectory + prefix + '/';

		var test = suite[i];
		/*
		"N" : 10000,
		"id" : [{"M" : 3, "strings" : false}, {"M" : 3, "strings" : false}],
		"value" : [{"M" : 100}, {"M" : 100}]
		*/

		var N = test.N;
		var M = 3;
		names = test.id.map(function(spec, i){ return "id_" + i;});

		var testName = "groupby.countmulti: " + N + "x" + M + "x" + names.length;
		tape(testName, generateTestCase(directory, names, ["value_0"]));
	}
});

function generateTestCase(directory, id_names, value_names){
	return function(t){
		t.plan(1);

		var names = id_names.concat(value_names);
		// load matrices from files
		load(directory, names, function(err, columns){

			floader.load(directory + "out.json", function(err, out){
				var expected = JSON.parse(out);

				var column_set = {};
				for (var i = 0; i < names.length; i++){
					var name = names[i];
					var column = columns[i];
					column_set[name] = column;
				}
				var frame = new Frame(column_set);

				var g = frame.groupbymulti(id_names);
				var actual = g.countmulti(value_names[0]);

				t.equals(JSON.stringify(actual), JSON.stringify(expected));
			});

		});
	};
}

function loadInt32Array(path, cb){
	return aloader.load(path, Int32Array, cb);
}

/* Load test matrices from JSON data, works in a browser (with XHR)
	assumes three files 'a.json', 'b.json' and 'c.json' in nested Array format.

callback = function(err, a, b, c)
*/
function load(directory, names, callback){

	// array of paths to matrix data files for current test
	var paths = names.map(function(name){ return directory + name + ".i32";});

	//console.log(testFiles);
	async.map(paths, loadInt32Array,
		function(err, results){

			if(err) return callback(err);

			callback(err, results);
		}
	);
};

/*
test("groupbymulti.sum", function(t){
	t.plan(1);
	var frame = new Frame({
		"id"  : [0, 0, 0, 1, 1, 0, 1, 0, 1],
		"value" : [1, 2, 2, 3, 1, 3, 4, 2, 1]
	});

	var expected = [10, 9];

	var g = frame.groupby("id");
	var actual = g.sum("value");

	t.equals(JSON.stringify(actual), JSON.stringify(expected));

});
*/

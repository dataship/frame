#!/usr/bin/env node
var spawn = require('child_process').spawn,
	async = require('async');


/*
./generate.py count/ count/small.json
./generate.py sum/ sum/small.json
*/

var tasks = [
	['generate.py', 'groupby.count/', 'groupby.count/small.json'],
	['generate.py', 'groupby.sum/',   'groupby.sum/small.json'],
	['generate.py', 'where.in.sum/',   'where.in.sum/small.json']
];
var options = {
    "cwd" : __dirname,
    "stdio": ["inherit", "inherit", "inherit"]};


async.eachSeries(tasks, function(task, callback){
		spawn('python', task, options).on('close', callback);
	},
	function(){
		// all done
});

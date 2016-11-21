var Benchtap = require('./benchtap'),
	Frame = require('../frame');

function randomIntArray(N, K){

	var data = [];

	for(var i = 0; i < N; i++){
		data.push(Math.random() * K | 0);
	}

	return data;
};

function randomFloatArray(N){

	var data = [];

	for(var i = 0; i < N; i++){
		data.push(Math.random() / Math.sqrt(N));
	}

	return data;
};

var benchtap = new Benchtap();


function createSetup(N, K){
	return function(event){
		// generate data
		var groupCol = randomIntArray(N, K);
		var valueCol = randomFloatArray(N);

		// create frame
		var columnDict = {
			"group-col" : groupCol,
			"reduce-col" : valueCol
		};

		this.frame = new Frame(columnDict);
	};
}

var N = 100000,
	K = 20;

var name = "groupby.sum: " + N + "x" + K;

benchtap.add(name, N, createSetup(N, K),function(){
	var group = this.frame.groupby("group-col");
	var result = group.reduce("reduce-col");
});

var N = 1000000,
	K = 20;

name = "groupby.sum: " + N + "x" + K;

benchtap.add(name, N, createSetup(N, K),function(){
	var group = this.frame.groupby("group-col");
	var result = group.reduce("reduce-col");
});

benchtap.run();

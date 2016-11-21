module.exports = {
	"Array" : {
		"int" : randomIntArray,
		"float" : randomFloatArray
	}

}

function randomIntArray(N, K){

	var data = [];

	for(var i = 0; i < N; i++){
		data.push(Math.random() * K | 0);
	}

	return data;
}

function randomFloatArray(N){

	var data = [];

	for(var i = 0; i < N; i++){
		data.push(Math.random() / Math.sqrt(N));
	}

	return data;
}

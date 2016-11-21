var Benchmark = require('benchmark');

/* Run benchmarks with tap output, compatible with testling and browserify
 */
function Benchtap(){
	this.suite = new Benchmark.Suite();
	this.pass = 0;
	this.fail = 0;
	var self = this;
	this.suite.on('complete', function(){
		printFooter(self.suite.length, self.pass, self.fail);
	});
}

module.exports = Benchtap;

Benchtap.prototype.add = function(name, N, setup, test){

	var self = this;

	var b = new Benchmark(name, test)
		.on('start', setup)
		.on('cycle', function(event) {
		})
		.on('complete', function(event) {
			var benchmark = this;
			if(benchmark.error){
				printError(event.currentTarget.id, benchmark.name, benchmark.error);
				self.fail++;
			} else {

				var pm = '\xb1',
					mu = '\xb5'
					size = benchmark.stats.sample.length;

				var mflops = benchmark.hz * N / 1e6;

				var info = Benchmark.formatNumber(mflops.toFixed(3)) + ' MFlops/sec ' +
					' ' + pm + benchmark.stats.rme.toFixed(2) + '% ' +
					' n = ' + size +
					' ' + mu + " = " + (benchmark.stats.mean * 1000).toFixed(0) + 'ms';

				printPass(event.currentTarget.id, benchmark.name, info);
				self.pass++;
			}
		});

	this.suite.add(b);
}

Benchtap.prototype.run = function(){
	printHeader();

	this.suite.run({ 'async': true });
}

function printHeader(){
	console.log("TAP version 13");
}

function printFooter(total, pass, fail){

	console.log("\n1.." + total);
	console.log("# tests " + total);
	console.log("# pass  " + pass);

	if(fail)
		console.log("# fail  " + fail);
	else
		console.log("\n# ok\n");
}

function printError(id, name, error){
	console.log("not ok " + id + " " + name);
	// show error
	console.log("  ---");
	console.log("  error: " + error);
	console.log("  ...");
}

function printPass(id, name, info){
	console.log("ok " + id + " " + name);
	console.log("# " + info);
}

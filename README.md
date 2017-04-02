# frame

a DataFrame for Javascript.

_crunch numbers in Node or the Browser_

## features
* Interactive performance (<100ms) on millions of rows
* Syntax similar to SQL and Pandas
* Compatible with `PapaParse` and [`BabyParse`](https://github.com/Rich-Harris/BabyParse)

## examples
Parse the [Iris](https://vincentarelbundock.github.io/Rdatasets/datasets.html)
dataset (with [`BabyParse`](https://github.com/Rich-Harris/BabyParse)) and create a `Frame` from the result.

```javascript
var baby = require('babyparse'),
    Frame = require('frame');

// parse the csv file
config = {"header" :true, "dynamicTyping" : true, "skipEmptyLines" : true};
iris = baby.parseFiles('iris.csv', config).data;

// create a frame from the parsed results
frame = new Frame(iris);
```
### groupby

Group on `Species` and find the average value (`mean`) for `Sepal.Length`.
```javascript
g = frame.groupby("Species");
g.mean("Sepal.Length");
```
```json
{ "virginica": 6.58799, "versicolor": 5.9360, "setosa": 5.006 }
```
Using the same grouping, find the average value for `Sepal.Width`.
```javascript
g.mean("Sepal.Width");
```
```json
{ "virginica": 2.97399, "versicolor": 2.770, "setosa": 3.4279 }
```

### where
Filter by `Species` value `virginica` then find the average.
```javascript
f = frame.where("Species", "virginica");
f.mean("Sepal.Length");
```
```json
6.58799
```
Get the number of rows that match the filter.
```javascript
f.count();
```
```json
50
```
Columns can be accessed directly, and the filter is applied.
```javascript
f["Species"]
```
```javascript
["virginica", "virginica", "virginica", ..., "virginica" ]
```
# tests
Hundreds of tests verify correctness on millions of data points (against a Pandas reference).

`npm run data && npm run test`

# benchmarks
`npm run benchmark`

typical performance on one million rows

operation | time
----------|------
`groupby` | 54ms
`where`   | 29ms
`sum`     | 5ms

# design goals and inspiration

 * compatibility with [feather](https://github.com/wesm/feather)

## interface

* pandas
* R
* Linq
* rethinkDB
* Matlab

## performance

* [datavore](https://github.com/StanfordHCI/datavore)

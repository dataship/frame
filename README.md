# frame

A lightweight, high performance Columnar Data Store disguised as a Data Frame

## tests
`npm run test`

## benchmarks
`npm run benchmark`

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

### results

`groupby` on 1 million rows in < 100ms

```
TAP version 13
# create: 100000x3
ok 1 : n = 48, µ = 1ms, ops = 110.565 M/sec ±7.53%
# create: 100000x3 (strings)
ok 2 : n = 48, µ = 2ms, ops = 61.730 M/sec ±12.56%
# create: 1000000x3
ok 3 : n = 25, µ = 26ms, ops = 38.276 M/sec ±29.70%
# create: 1000000x3 (strings)
ok 4 : n = 20, µ = 33ms, ops = 30.760 M/sec ±25.11%
# groupby.sum: 100000x3
ok 5 : n = 58, µ = 6ms, ops = 34.977 M/sec ±3.02%
# groupby.sum: 100000x3 (strings)
ok 6 : n = 57, µ = 7ms, ops = 29.634 M/sec ±2.55%
# groupby.sum: 1000000x3
ok 7 : n = 29, µ = 64ms, ops = 31.258 M/sec ±5.44%
# groupby.sum: 1000000x3 (strings)
ok 8 : n = 26, µ = 75ms, ops = 26.578 M/sec ±5.26%
# sum: 100000x3
ok 9 : n = 59, µ = 4ms, ops = 28.524 M/sec ±0.35%
# sum: 1000000x3
ok 10 : n = 39, µ = 36ms, ops = 27.864 M/sec ±0.28%

1..10
# tests 10
# pass  10

# ok
```

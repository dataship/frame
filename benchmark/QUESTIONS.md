
### Why are my dv results not consistent with their benchmark webpage?
because it slows down with consecutive runs, dropping to a quarter of initial performance.

ok 1 table.query.sum: 1000000x3
# 12.019 MFlops/sec ±16.51%  n = 15 µ = 83ms : [0.022,0.02225,0.0935,0.092,0.0925,0.0925,0.09325,0.092,0.093,0.09275,0.09275,0.09225,0.09275,0.092,0.0925]


### Why is dv faster initially?

### Is the dv setup longer?

### Can I make Frame as fast as dv by encoding the strings?
likely it will give a 3x speedup.

#### integers
ok 1 groupby.sum: 1000000x3
# 13.952 MFlops/sec ±2.01%  n = 29 µ = 72ms : [0.0645,0.0625,0.0635,0.062,0.0775,0.0725,0.0715,0.073,0.0725,0.073,0.0735,0.0745,0.0725,0.0725,0.074,0.073,0.0715,0.077,0.072,0.071,0.072,0.0715,0.0725,0.0735,0.0725,0.073,0.0745,0.0715,0.0735]

#### strings
ok 1 groupby.sum: 1000000x3
# 4.120 MFlops/sec ±3.74%  n = 14 µ = 243ms : [0.239,0.235,0.232,0.234,0.267,0.267,0.24,0.235,0.235,0.236,0.279,0.233,0.233,0.233]


### Is the FrameIndex.reduce faster than dv.query, when Frame.groupby has already been run?

# merk-go
Go implementation of merk which is A High-Performance Merkle AVL Tree created by nomic-io: https://github.com/nomic-io/merk/blob/develop/docs/algorithms.md

## Benchmarks
A benchmark was done using macbook pro 2020.
CPU: 1.4 GHz Quad-Core Intel Core i5
MEM: 16 GB 2133 MHz LPDDR3

The benchmark task was processing 100k batch which is composed of 50 % insert and 40 % update and 10% delete cases. Mark is compared with ordinary avl tree.(I referred to [wavelet](https://github.com/perlin-network/wavelet/tree/master/avl))

As a result, Merk is 5 times faster in no commit case. And 2 times faster in commit case.


### Result: Merk
```
make bench
...
pkg: github.com/tak1827/merk-go/merk
BenchmarkApply-8            94   502389691 ns/op  175322128 B/op   2983036 allocs/op
BenchmarkCommit-8           16  2590092891 ns/op  2503110626 B/op 14300158 allocs/op
```

### Result: AVL
```
make bench
....
pkg: github.com/tak1827/merk-go/avl
BenchmarkApply-8            16   928723790 ns/op  388644402 B/op   2393708 allocs/op
BenchmarkCommit-8            8  1807893350 ns/op  938078886 B/op   7542973 allocs/op
```

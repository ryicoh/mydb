# mydb

[![Go](https://github.com/ryicoh/mydb/actions/workflows/go.yml/badge.svg)](https://github.com/ryicoh/mydb/actions/workflows/go.yml)

`mydb` is a simple key-value store written in golang.
For now, the data is stored in persistent storage and has its location in the built-in map.


It supports SET/GET/DEL of RESP, the Redis protocol, so it can be used as follows.

```bash
$ go run ./cmd/mydbd

$ redis-cli -p 9888
127.0.0.1:9888> set hello world
OK

127.0.0.1:9888> get hello
world

127.0.0.1:9888> del hello
OK
```

## Benchmark

This is results of running `redis-benchmark`.

```bash
ryicoh@ryicohs-MacBook-Air mydb % redis-benchmark -n 100000 -c 100 -q -t set,get -d 1000 -p 9888
ERROR: unsupport command `CONFIG`
ERROR: failed to fetch CONFIG from 127.0.0.1:9888
WARNING: Could not fetch server CONFIG
SET: 139860.14 requests per second, p50=0.471 msec
GET: 164744.64 requests per second, p50=0.311 msec
```

## TODO

- [ ] Get range of keys.
- [x] Benchmark.
- [ ] Transaction.

# mydb

[![Go](https://github.com/ryicoh/mydb/actions/workflows/go.yml/badge.svg)](https://github.com/ryicoh/mydb/actions/workflows/go.yml)

`mydb` is a simple key-value store written in golang.
For now, the data is stored in persistent storage and has its location in the built-in map.


It supports SET, GET and DEL of RESP, the Redis protocol, so it can be used as follows.

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


## TODO

- [ ] Get range of keys.
- [ ] Benchmark.
- [ ] Transaction.

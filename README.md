[![license](https://img.shields.io/github/license/yandex/temporal-over-ydb)](https://github.com/yandex/temporal-over-ydb/blob/main/LICENSE)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/yandex/temporal-over-ydb)
![stability-wip](https://img.shields.io/badge/stability-wip-lightgrey.svg)

# temporal-over-ydb

**temporal-over-ydb** is an implementation of a custom [Temporal](https://temporal.io) persistence layer using [YDB](https://ydb.tech),
a distributed SQL DBMS.

It is still a work in active progress and is not ready for production use.

## How to run tests

Clone and patch Temporal source code:
```
git clone https://github.com/temporalio/temporal.git
cd ./temporal
git checkout v1.24.3
```

Run docker-compose:
```
cd ./tests
docker-compose -f ./docker-compose.yml up -d
```

Run persistence unit tests:
```
cd ./tests/persistencetests
go mod tidy
YDB_DATABASE=local YDB_ENDPOINT="localhost:2136" YDB_ANONYMOUS_CREDENTIALS=1 go test . -v
```


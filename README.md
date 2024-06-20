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
git checkout v1.22.3

# This patch allows setting custom advanced visibility persistence,
# see https://github.com/temporalio/temporal/pull/4871
curl https://github.com/temporalio/temporal/compare/634a5a1223a8b2bc00b9320d17ed1f1b67182fb1..3f0ad69ad521ffb07e1bbdea1aa4da0adeb3be7e.diff -Lso- | git apply

# Since the suites in the tests package are not exportable and not configurable with
# custom persistence, for now we just patch them to use YDB:
git apply ../tests/functests.patch
cd ./tests
cp ../../tests/functests.go.mod ./go.mod
go mod tidy
```

Run docker-compose:
```
cd ./tests
docker-compose -f ./docker-compose.yml up -d
```

Run functional server tests:
```
cd ./temporal/tests
# Specific suite:
go test . -persistenceType=nosql -persistenceDriver=ydb -run 'TestIntegrationSuite'
# Or all of them:
go test . -persistenceType=nosql -persistenceDriver=ydb
```

Run persistence unit tests:
```
cd ./tests/persistencetests
go mod tidy
go test .
```


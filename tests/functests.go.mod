module go.temporal.io/server/tests

go 1.19

require (
        github.com/yandex/temporal-over-ydb v0.0.0-00010101000000-000000000000
        github.com/yandex/temporal-over-ydb/tests/persistencetests v0.0.0-00010101000000-000000000000
        go.temporal.io/server v0.0.0-00010101000000-000000000000
)

replace github.com/yandex/temporal-over-ydb/tests/persistencetests => ../../tests/persistencetests

replace github.com/yandex/temporal-over-ydb => ../../

replace go.temporal.io/server => ../
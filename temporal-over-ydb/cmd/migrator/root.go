package main

import (
	"context"
	"database/sql"

	"github.com/spf13/cobra"
	ydbenv "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func init() {
	rootCmd.PersistentFlags().StringVar(&host, "host", "", "host")
	if err := rootCmd.MarkPersistentFlagRequired("host"); err != nil {
		panic(err)
	}

	rootCmd.PersistentFlags().StringVar(&port, "port", "", "port")
	if err := rootCmd.MarkPersistentFlagRequired("port"); err != nil {
		panic(err)
	}

	rootCmd.PersistentFlags().StringVar(&dbName, "db", "", "database name")
	if err := rootCmd.MarkPersistentFlagRequired("db"); err != nil {
		panic(err)
	}

	rootCmd.PersistentFlags().StringVar(&prefix, "prefix", "", "table path prefix")

	rootCmd.PersistentFlags().BoolVar(&withSecure, "secure", false, "Use secure connection (default false)")
}

var (
	host       string
	port       string
	dbName     string
	withSecure bool
	prefix     string

	db *sql.DB

	rootCmd = &cobra.Command{
		Use:   "ydb-migrator",
		Short: "A CLI tool for managing YDB migrations",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			endpoint := host + ":" + port
			driver, err := ydb.Open(cmd.Context(),
				sugar.DSN(endpoint, dbName, sugar.WithSecure(withSecure)),
				ydbenv.WithEnvironCredentials(context.Background()),
			)
			if err != nil {
				return err
			}

			tablePrefix := dbName + "/" + prefix
			connector, err := ydb.Connector(driver,
				ydb.WithTablePathPrefix(tablePrefix),
				ydb.WithDefaultQueryMode(ydb.ScriptingQueryMode),
				ydb.WithFakeTx(ydb.ScriptingQueryMode),
				ydb.WithNumericArgs(),
				ydb.WithAutoDeclare(),
			)
			if err != nil {
				return err
			}

			db = sql.OpenDB(connector)

			return nil
		},
	}
)

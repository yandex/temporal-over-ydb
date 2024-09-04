package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
)

func main() {
	app := buildCLI()
	_ = app.Run(os.Args)
}

func buildCLI() *cli.App {
	app := cli.NewApp()
	app.Name = "temporal-ydb-tool"
	app.Usage = "Temporal ydb database schema setup tool"
	app.Version = headers.ServerVersion
	app.ArgsUsage = " "
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "endpoint",
			Aliases: []string{"ep"},
			Value:   "",
			Usage:   "endpoint to ydb cluster",
			EnvVars: []string{"YDB_ENDPOINT"},
		},
		&cli.StringFlag{
			Name:    "database",
			Aliases: []string{"db"},
			Value:   "",
			Usage:   "database of ydb cluster",
			EnvVars: []string{"YDB_DBNAME"},
		},
		&cli.StringFlag{
			Name:    "folder",
			Aliases: []string{"f"},
			Value:   "",
			Usage:   "path to temporal tables",
			EnvVars: []string{"YDB_TABLE_PATH"},
		},
		&cli.StringFlag{
			Name:    "token",
			Aliases: []string{"t"},
			Value:   "",
			Usage:   "authentication token",
			EnvVars: []string{"YDB_TOKEN"},
		},
		&cli.BoolFlag{
			Name:  "use-ssl",
			Value: false,
			Usage: "use ssl protocol",
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:      "ping",
			Usage:     "Perform connection check to ydb cluster",
			ArgsUsage: " ",
			Action: func(c *cli.Context) error {
				logger := log.NewZapLogger(log.BuildZapLogger(log.Config{}))
				cfg := xydb.Config{
					Endpoint: c.String("endpoint"),
					Database: c.String("database"),
					Folder:   c.String("folder"),
					Token:    c.String("token"),
					UseSSL:   c.Bool("use-ssl"),
				}
				client, err := xydb.NewClient(c.Context, cfg, logger, nil)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to setup ydb client. Error: %v", err), 1)
				}
				_ = client.Close(c.Context)
				return cli.Exit("Ok.", 0)
			},
		},
		{
			Name:      "update-schema",
			Usage:     "Run schema DDL statements",
			ArgsUsage: " ",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "path",
					Value: "",
					Usage: "path to ddl statements file",
				},
			},
			Action: func(c *cli.Context) error {
				logger := log.NewZapLogger(log.BuildZapLogger(log.Config{}))
				cfg := xydb.Config{
					Endpoint: c.String("endpoint"),
					Database: c.String("database"),
					Folder:   c.String("folder"),
					Token:    c.String("token"),
					UseSSL:   c.Bool("use-ssl"),
				}

				logger.Info("Setup ydb client")
				client, err := xydb.NewClient(c.Context, cfg, logger, nil)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to setup ydb client. Error: %v", err), 1)
				}
				defer client.Close(c.Context)

				logger.Info("Read scripts")
				scriptPath := c.String("path")
				scriptBytes, err := os.ReadFile(scriptPath)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to read migration scripts. Error: %v", err), 1)
				}

				prefix := ""
				if client.Folder != "" {
					prefix = fmt.Sprintf(`PRAGMA TablePathPrefix = "/%s/%s";`, client.Database, client.Folder)
				} else {
					prefix = fmt.Sprintf(`PRAGMA TablePathPrefix = "/%s";`, client.Database)
				}
				script := fmt.Sprintf("%s\n%s", prefix, string(scriptBytes))
				fmt.Printf("%s", script)

				logger.Info("Execute scripts")
				_, err = client.DB.
					Scripting().
					Execute(c.Context, script, table.NewQueryParameters())
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to execute migration scripts. Error: %v", err), 1)
				}

				return cli.Exit("Migrations complete.", 0)
			},
		},
	}

	return app
}

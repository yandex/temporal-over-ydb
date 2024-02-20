package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli"
	"github.com/yandex/temporal-over-ydb/persistencelite"
	"github.com/yandex/temporal-over-ydb/persistencelite/conn"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

func BuildCLIOptions() *cli.App {
	app := cli.NewApp()
	app.Name = "rqlitetool"
	app.Version = "0.0.1"

	logger := log.NewCLILogger()

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "url",
			Value:  "http://localhost:4000",
			Usage:  "RQlite conn URLs",
			EnvVar: "RQL_CONN_URLS",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "setup-schema",
			Aliases: []string{"setup"},
			Usage:   "setup initial version of sql schema",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:     "schema-file",
					Usage:    "path to the .sql schema file",
					Required: true,
				},
			},
			Action: func(c *cli.Context) {
				url := c.GlobalString("url")
				rqliteFactory, err := conn.NewRqliteFactoryFromConnURL(metrics.NoopMetricsHandler, url, 1)
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to create RQLite factory: %s", err))
					os.Exit(1)
				}

				path := c.String("schema-file")

				if err = persistencelite.SetupRQLiteDatabase(rqliteFactory, path); err != nil {
					logger.Error(fmt.Sprintf("Failed to setup RQLite: %s", err))
					os.Exit(1)
				}

				fmt.Println("succeeded!")
			},
		},
		{
			Name:    "drop-database",
			Aliases: []string{"drop"},
			Usage:   "drops a database",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "force",
					Usage: "don't prompt for confirmation",
				},
			},
			Action: func(c *cli.Context) {
				drop := c.Bool("force")
				url := c.GlobalString("url")
				if !drop {
					fmt.Printf("Are you sure you want to drop database %q (y/N)? ", url)
					y := ""
					_, _ = fmt.Scanln(&y)
					if y == "y" || y == "Y" {
						drop = true
					}
				}
				if drop {
					rqliteFactory, err := conn.NewRqliteFactoryFromConnURL(metrics.NoopMetricsHandler, url, 1)
					if err != nil {
						logger.Error(fmt.Sprintf("Failed to create RQLite factory: %s", err))
						os.Exit(1)
					}
					err = persistencelite.TearDownTestRQLiteDatabase(rqliteFactory)
					if err != nil {
						logger.Error(fmt.Sprintf("Failed to setup RQLite: %s", err))
						os.Exit(1)
					}
				}
			},
		},
	}

	return app
}

func RunTool(args []string) error {
	app := BuildCLIOptions()
	return app.Run(args)
}

func main() {
	if err := RunTool(os.Args); err != nil {
		os.Exit(1)
	}
}

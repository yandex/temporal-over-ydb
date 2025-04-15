package main

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/build"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/temporal"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb"
	ydbrows "github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/rows"
)

func main() {
	app := buildCLI()
	_ = app.Run(os.Args)
}

func buildCLI() *cli.App {
	app := cli.NewApp()
	app.Name = "temporal"
	app.Usage = "Temporal over YDB server"
	app.Version = headers.ServerVersion
	app.ArgsUsage = " "
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "root",
			Aliases: []string{"r"},
			Value:   ".",
			Usage:   "root directory of execution environment",
			EnvVars: []string{config.EnvKeyRoot},
		},
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Value:   "config",
			Usage:   "config dir path relative to root",
			EnvVars: []string{config.EnvKeyConfigDir},
		},
		&cli.StringFlag{
			Name:    "env",
			Aliases: []string{"e"},
			Value:   "development",
			Usage:   "runtime environment",
			EnvVars: []string{config.EnvKeyEnvironment},
		},
		&cli.StringFlag{
			Name:    "zone",
			Aliases: []string{"az"},
			Usage:   "availability zone",
			EnvVars: []string{config.EnvKeyAvailabilityZone, config.EnvKeyAvailabilityZoneTypo},
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:      "start",
			Usage:     "Start Temporal server",
			ArgsUsage: " ",
			Flags: []cli.Flag{
				&cli.StringSliceFlag{
					Name:    "service",
					Aliases: []string{"svc"},
					Value:   cli.NewStringSlice(temporal.Services...),
					Usage:   "service(s) to start",
				},
			},
			Action: func(c *cli.Context) error {
				env := c.String("env")
				zone := c.String("zone")
				configDir := path.Join(c.String("root"), c.String("config"))
				services := c.StringSlice("service")
				allowNoAuth := c.Bool("allow-no-auth")

				// For backward compatibility to support old flag format (i.e. `--services=frontend,history,matching`).
				if c.IsSet("services") {
					fmt.Println("WARNING: --services flag is deprecated. Specify multiply --service flags instead.")
					services = strings.Split(c.String("services"), ",")
				}

				cfg, err := config.LoadConfig(env, configDir, zone)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to load configuration: %v.", err), 1)
				}

				// XXX
				ydbrows.NumHistoryShards = int(cfg.Persistence.NumHistoryShards)

				logger := log.NewZapLogger(log.BuildZapLogger(cfg.Log))
				logger.Info("Build info.",
					tag.NewTimeTag("git-time", build.InfoData.GitTime),
					tag.NewStringTag("git-revision", build.InfoData.GitRevision),
					tag.NewBoolTag("git-modified", build.InfoData.GitModified),
					tag.NewStringTag("go-arch", build.InfoData.GoArch),
					tag.NewStringTag("go-os", build.InfoData.GoOs),
					tag.NewStringTag("go-version", build.InfoData.GoVersion),
					tag.NewBoolTag("cgo-enabled", build.InfoData.CgoEnabled),
					tag.NewStringTag("server-version", headers.ServerVersion),
					tag.NewBoolTag("debug-mode", debug.Enabled),
				)

				var dynamicConfigClient dynamicconfig.Client
				if cfg.DynamicConfigClient != nil {
					dynamicConfigClient, err = dynamicconfig.NewFileBasedClient(cfg.DynamicConfigClient, logger, temporal.InterruptCh())
					if err != nil {
						return cli.Exit(fmt.Sprintf("Unable to create dynamic config client. Error: %v", err), 1)
					}
				} else {
					dynamicConfigClient = dynamicconfig.NewNoopClient()
					logger.Info("Dynamic config client is not configured. Using noop client.")
				}

				authorizer, err := authorization.GetAuthorizerFromConfig(
					&cfg.Global.Authorization,
				)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to instantiate authorizer. Error: %v", err), 1)
				}
				if authorization.IsNoopAuthorizer(authorizer) && !allowNoAuth {
					logger.Warn(
						"Not using any authorizer and flag `--allow-no-auth` not detected. " +
							"Future versions will require using the flag `--allow-no-auth` " +
							"if you do not want to set an authorizer.",
					)
				}

				claimMapper, err := authorization.GetClaimMapperFromConfig(&cfg.Global.Authorization, logger)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to instantiate claim mapper: %v.", err), 1)
				}

				s, err := temporal.NewServer(
					temporal.ForServices(services),
					temporal.WithConfig(cfg),
					temporal.WithDynamicConfigClient(dynamicConfigClient),
					temporal.WithLogger(logger),
					temporal.InterruptOn(temporal.InterruptCh()),
					temporal.WithAuthorizer(authorizer),
					temporal.WithClaimMapper(func(cfg *config.Config) authorization.ClaimMapper {
						return claimMapper
					}),
					temporal.WithCustomDataStoreFactory(ydb.NewYDBAbstractDataStoreFactory()),
				)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to create server. Error: %v.", err), 1)
				}

				err = s.Start()
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to start server. Error: %v", err), 1)
				}
				return cli.Exit("All services are stopped.", 0)
			},
		},
	}
	return app
}

package main

import (
	"log"

	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(upCmd)

	upCmd.Flags().StringVar(&versionTable, "version-table", "", "Version table name")
	if err := upCmd.MarkFlagRequired("version-table"); err != nil {
		panic(err)
	}

	upCmd.Flags().StringVar(&schemaDir, "schema-dir", "", "Schema dir")
	if err := upCmd.MarkFlagRequired("schema-dir"); err != nil {
		panic(err)
	}
}

var (
	versionTable string
	schemaDir    string

	upCmd = &cobra.Command{
		Use:   "up",
		Short: "Apply all up migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			goose.SetTableName(versionTable)

			if err := goose.SetDialect("ydb"); err != nil {
				return err
			}

			if err := goose.Up(db, schemaDir); err != nil {
				return err
			}

			log.Println("Migrations applied successfully.")
			return nil
		},
	}
)

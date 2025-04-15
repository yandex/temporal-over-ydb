package main

import (
	"log"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(pingCmd)
}

var (
	pingCmd = &cobra.Command{
		Use:   "ping",
		Short: "Check if the database is available",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := db.PingContext(cmd.Context()); err != nil {
				return err
			}
			log.Println("Database is reachable.")
			return nil
		},
	}
)

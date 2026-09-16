package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/duo-moon/airflow-metric-cli/internal/version"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print build version",
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "afmetric %s (commit %s, built %s)\n",
				version.Version, version.Commit, version.Date)
			return err
		},
	}
}

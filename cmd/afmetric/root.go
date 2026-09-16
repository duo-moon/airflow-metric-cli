package main

import "github.com/spf13/cobra"

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "afmetric",
		Short:         "Airflow metrics dashboard in the terminal",
		Long:          "afmetric polls an Airflow REST API and renders a real-time TUI dashboard of cluster health, DAG runs and task instances.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	cmd.PersistentFlags().String("api-version", "", "Airflow REST API dialect: v1 (Connexion, /api/v1) or v2 (FastAPI, /api/v2). Default: v1.")

	cmd.AddCommand(
		newVersionCmd(),
		newRunCmd(),
		newPingCmd(),
	)

	return cmd
}

// Package skillscassettecmder provides the skills-cassette command surface.
package skillscassettecmder

import (
	"github.com/papercomputeco/skills-cassette/internal/server"
	"github.com/spf13/cobra"
)

// Version is overridden by release builds when desired.
var Version = "dev"

// NewSkillsCassetteCmd creates the root command.
func NewSkillsCassetteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "skills-cassette",
		Short:         "Generate reusable skills from Tapes sessions",
		Version:       Version,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newServeCmd())
	return cmd
}

func newServeCmd() *cobra.Command {
	address := "127.0.0.1:8080"
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the health server",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return server.ListenAndServe(cmd.Context(), address)
		},
	}
	cmd.Flags().StringVar(&address, "listen", address, "listen address")
	return cmd
}

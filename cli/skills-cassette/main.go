package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	skillscassettecmder "github.com/papercomputeco/skills-cassette/cmd/skills-cassette"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run carries the signal context and its cleanup, so main's os.Exit can never
// skip the deferred stop.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return skillscassettecmder.NewSkillsCassetteCmd().ExecuteContext(ctx)
}

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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := skillscassettecmder.NewSkillsCassetteCmd().ExecuteContext(ctx); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

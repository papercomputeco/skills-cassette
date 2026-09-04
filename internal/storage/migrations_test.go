package storage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("migration deadlock retry", func() {
	deadlock := func() error {
		return &pgconn.PgError{Code: pgDeadlockDetected, Message: "deadlock detected"}
	}

	It("replays a deadlocked attempt and stops at the bound", func() {
		calls := 0
		err := retryMigration(context.Background(), 3, time.Millisecond, func(context.Context) error {
			calls++
			return deadlock()
		})
		Expect(isMigrationDeadlock(err)).To(BeTrue())
		Expect(calls).To(Equal(3), "the bound is the total number of attempts")
	})

	It("returns success from a later attempt", func() {
		calls := 0
		err := retryMigration(context.Background(), 3, time.Millisecond, func(context.Context) error {
			calls++
			if calls < 2 {
				return deadlock()
			}
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(calls).To(Equal(2))
	})

	It("does not retry errors other than a deadlock report", func() {
		calls := 0
		other := &pgconn.PgError{Code: pgUniqueViolation}
		err := retryMigration(context.Background(), 3, time.Millisecond, func(context.Context) error {
			calls++
			return other
		})
		Expect(errors.Is(err, other)).To(BeTrue())
		Expect(calls).To(Equal(1))
		Expect(isMigrationDeadlock(errors.New("deadlock detected"))).To(BeFalse(),
			"only SQLSTATE 40P01 counts, never message text")
	})

	It("stops waiting when the context is canceled", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		err := retryMigration(ctx, 3, time.Hour, func(context.Context) error {
			calls++
			return deadlock()
		})
		Expect(isMigrationDeadlock(err)).To(BeTrue())
		Expect(calls).To(Equal(1))
	})
})

package storagetest

import (
	"context"

	"github.com/papercomputeco/skills-cassette/internal/storage"
)

// DropSchema removes a test-private cassette schema with CASCADE while holding
// the published-contract advisory lock, in one transaction. Test packages run
// in parallel against one Postgres, and the skills_contract_v1 views are
// defined over whichever private schema migrated last: a bare
// DROP SCHEMA ... CASCADE takes exclusive locks on those views without the
// lock and deadlocks against a migration in another package that holds the
// lock while replacing them. Every test teardown must use this instead of
// issuing DROP SCHEMA itself; storage's own in-package tests call
// storage.DropPrivateSchema directly because they cannot import this package.
func DropSchema(ctx context.Context, executor storage.SchemaDropper, schema string) error {
	return storage.DropPrivateSchema(ctx, executor, schema)
}

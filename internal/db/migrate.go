// Package db bundles the DB schema (golang-migrate) and the sqlc-generated
// query layer (sub-package generated).
package db

import (
	"embed"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies all pending up-migrations to the given database.
// databaseURL must be a pgx URL, e.g. "pgx5://user:pass@host:5432/lndg?sslmode=disable".
// ErrNoChange (nothing to apply) is treated as success.
func Migrate(databaseURL string) error {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("migrate: load migrations: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, databaseURL)
	if err != nil {
		return fmt.Errorf("migrate: init: %w", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("migrate: up: %w", err)
	}
	return nil
}

// Package db bundles the DB schema (golang-migrate) and the sqlc-generated
// query layer (sub-package generated).
package db

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies all pending up-migrations to the given database.
// databaseURL must be a pgx URL, e.g. "pgx5://user:pass@host:5432/lndg?sslmode=disable".
// ErrNoChange (nothing to apply) is treated as success.
//
// A database created by the Django app has no migrate version yet: 000001 is
// its schema (gui migrations up to 0061), so it is marked as applied instead
// of run. An older Django schema is refused.
func Migrate(databaseURL string) error {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("migrate: load migrations: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, databaseURL)
	if err != nil {
		return fmt.Errorf("migrate: init: %w", err)
	}
	if _, _, err := m.Version(); errors.Is(err, migrate.ErrNilVersion) {
		django, err := djangoSchema(databaseURL)
		if err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
		if django {
			if err := m.Force(1); err != nil {
				return fmt.Errorf("migrate: baseline Django schema: %w", err)
			}
		}
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("migrate: up: %w", err)
	}
	return nil
}

// djangoSchema reports whether the database was set up by the Django app, and
// fails when its gui migrations stop before 0061 (the schema of 000001).
func djangoSchema(databaseURL string) (bool, error) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres"+strings.TrimPrefix(databaseURL, "pgx5"))
	if err != nil {
		return false, err
	}
	defer conn.Close(ctx)
	var django, at0061 bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass('public.django_migrations') IS NOT NULL`).Scan(&django); err != nil || !django {
		return false, err
	}
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM django_migrations WHERE app = 'gui' AND name = '0061_graphprobelog_restore_probelog')`).Scan(&at0061); err != nil {
		return false, err
	}
	if !at0061 {
		return false, errors.New("the Django schema is older than gui 0061: run `python manage.py migrate` in the Python app first")
	}
	return true, nil
}

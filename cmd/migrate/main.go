// Command migrate applies the DB schema using golang-migrate with the
// embedded migrations from internal/db/migrations. DATABASE_URL is read
// from config (env or lndg.conf).
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/db"
)

func main() {
	url := config.Get().DATABASE_URL
	// The golang-migrate pgx/v5 driver expects the "pgx5://" scheme; config
	// uses "postgres://" (matching pgxpool). Rewrite the scheme accordingly.
	switch {
	case strings.HasPrefix(url, "postgresql://"):
		url = "pgx5" + strings.TrimPrefix(url, "postgresql")
	case strings.HasPrefix(url, "postgres://"):
		url = "pgx5" + strings.TrimPrefix(url, "postgres")
	}
	if err := db.Migrate(url); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	fmt.Println("migrate: schema up to date")
}

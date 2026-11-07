package repository

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // registers the postgres driver
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrations returns the embedded SQL migration files.
func Migrations() (fs.FS, error) {
	sub, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("migrations: %w", err)
	}
	return sub, nil
}

// NewMigrator builds a golang-migrate instance over the embedded migrations.
func NewMigrator(databaseURL string) (*migrate.Migrate, error) {
	source, err := Migrations()
	if err != nil {
		return nil, err
	}
	driver, err := iofs.New(source, ".")
	if err != nil {
		return nil, fmt.Errorf("migrations: source: %w", err)
	}
	migrator, err := migrate.NewWithSourceInstance("iofs", driver, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("migrations: database: %w", err)
	}
	return migrator, nil
}

// MigrateUp applies all pending migrations. It is a no-op when the schema is
// already current.
func MigrateUp(databaseURL string) error {
	migrator, err := NewMigrator(databaseURL)
	if err != nil {
		return err
	}
	defer closeMigrator(migrator)
	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrations: up: %w", err)
	}
	return nil
}

// MigrateDown rolls every migration back.
func MigrateDown(databaseURL string) error {
	migrator, err := NewMigrator(databaseURL)
	if err != nil {
		return err
	}
	defer closeMigrator(migrator)
	if err := migrator.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrations: down: %w", err)
	}
	return nil
}

func closeMigrator(migrator *migrate.Migrate) {
	sourceErr, databaseErr := migrator.Close()
	_ = sourceErr
	_ = databaseErr
}

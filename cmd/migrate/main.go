// Command migrate applies or rolls back the embedded SQL migrations against
// DATABASE_URL. It is wired to `make migrate-up` / `make migrate-down`.
package main

import (
	"flag"
	"fmt"
	"os"

	"go.uber.org/zap"

	"social_service/internal/repository"
)

func main() {
	direction := flag.String("direction", "up", "migration direction: up or down")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "migrate: init logger: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = logger.Sync() }()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		logger.Error("DATABASE_URL is required but was not set",
			zap.String("operation", "migrate"), zap.String("status", "error"))
		os.Exit(1)
	}

	switch *direction {
	case "up":
		err = repository.MigrateUp(databaseURL)
	case "down":
		err = repository.MigrateDown(databaseURL)
	default:
		logger.Error("unknown migration direction",
			zap.String("operation", "migrate"), zap.String("status", "error"), zap.String("direction", *direction))
		os.Exit(1)
	}
	if err != nil {
		logger.Error("migration failed",
			zap.String("operation", "migrate"), zap.String("status", "error"),
			zap.String("direction", *direction), zap.String("detail", err.Error()))
		os.Exit(1)
	}
	logger.Info("migrations applied",
		zap.String("operation", "migrate"), zap.String("status", "ok"), zap.String("direction", *direction))
}

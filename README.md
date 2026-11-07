# social_service

Friend requests, friendships, and private conversations (direct / group) for the
Community Chat & Voice Platform.

- Behavioral and data specification: [`docs/architecture.md`](docs/architecture.md)
- HTTP contract: [`docs/openapi_spec.yaml`](docs/openapi_spec.yaml)
- Library, layout, and testing conventions: [`docs/engineering_guide.md`](docs/engineering_guide.md)
- Acceptance criteria: [`docs/user_stories.md`](docs/user_stories.md)

## Quick start

```bash
make install     # go mod download
make test        # go test ./... -race -count=1 -parallel 4
make build       # static binary in bin/server
make run         # go run ./cmd/server (reads the environment)
make migrate-up  # apply SQL migrations from internal/repository/migrations
make lint        # golangci-lint run ./...
```

Configuration is 12-factor (environment only, validated at startup). See
`.env.example` and section 6 of the engineering guide.

## Layout

```
cmd/server            entrypoint: config, wiring, graceful shutdown
cmd/migrate           golang-migrate runner for the embedded SQL migrations
internal/config       typed, validated configuration
internal/httpapi      router, middleware (auth, trace, metrics), handlers, probes
internal/friendship   friend-request lifecycle + friendship domain service
internal/conversation direct/group conversation domain service
internal/repository   PostgreSQL store, in-memory store, SQL migrations
internal/events       event envelope, outbox records, Kafka publisher
internal/models       domain types, DTOs, errors, clock, cursor, trace ids
tests/{unit,integration,mocks}
```

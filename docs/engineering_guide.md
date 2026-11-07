# Engineering Guide — `social_service`

Persistent technical memory for implementing and evolving this service.
Codebase-facing decisions (runtime, libraries, testing, conventions,
configuration, observability) live here; the behavioral and data
specification lives in `docs/architecture.md` and `docs/user_stories.md`.

---

## 1. Runtime & Toolchain

| Aspect | Value |
|---|---|
| Language | Go (module name: `social_service`, module path = service name, no VCS host prefix) |
| Runtime version | Latest stable Go toolchain (Dockerfile base `golang:1.26-alpine` builder stage) |
| Package manager | Go modules (`go.mod` / `go.sum`) |
| Build entrypoint | `make build` — `CGO_ENABLED=0` static binary from `./cmd/server` |
| Test entrypoint | `make test` — `go test ./... -race -count=1 -parallel 4` |
| Lint entrypoint | `make lint` — `golangci-lint run ./...` |
| Run locally | `make run` — `go run ./cmd/server` |
| Migrations | `make migrate-up` / `make migrate-down` (golang-migrate against `DATABASE_URL`) |

Always run `make install` (`go mod download`) first on a fresh workspace; add dependencies only via `go get <module>@latest` + `go mod tidy`, never by hand-editing manifests. New dependency versions are resolved through the package manager (plus `technical_research_agent` for unfamiliar APIs) — never guessed.

---

## 2. Libraries & External Provider SDKs

| Concern | Package / Library | Source | Usage Notes |
|---|---|---|---|
| HTTP framework | `github.com/gin-gonic/gin` | language-skill | Handlers stay thin; bind + validate at the boundary, delegate to domain services. |
| Input validation | `go-playground/validator/v10` (bundled with Gin bindings) | language-skill | Validate request structs with explicit tags; business rules stay in the domain layer. |
| PostgreSQL driver | `github.com/jackc/pgx/v5` (via `pgxpool`) | language-skill | Parameterized queries only — never `fmt.Sprintf` into SQL. Use `pgx.Tx` for the state+outbox transaction. |
| Migrations | `github.com/golang-migrate/migrate/v4` | language-skill | SQL migration files under `internal/repository/migrations`; run in the deploy pipeline. |
| Kafka client | `github.com/IBM/sarama` | language-skill | Producer only. Sync produce with `RequireOne` acks; enable idempotent producer; envelope mirrored into headers (`trace_id`, `event_type`). |
| Access-token verification | `github.com/golang-jwt/jwt/v5` + platform JWKS/PEM handling | language-skill | Local verification of identity_service-issued RS256/EdDSA access tokens against the current + next public keys from config (key rotation support). No network calls at request time. |
| Logging | `go.uber.org/zap` (JSON production encoder) | language-skill | Logger injected via dependency injection / context; structured fields only. |
| Metrics | `github.com/prometheus/client_golang/prometheus` | language-skill | Register RED + business metrics at startup; low-cardinality labels. |
| UUIDs | `github.com/google/uuid` | language-skill | v4 generation; also derive the 32-hex `trace_id` (uuid without hyphens, or random 16 bytes hex). |
| Tracing context | W3C `traceparent` parsing (`go.opentelemetry.io/otel/trace` header propagation or minimal in-house parser) | tech-lead-research | Extract/propagate `trace_id` on HTTP and into the event envelope; mint when absent. |
| Testing assertions | `github.com/stretchr/testify` (`assert`/`require`) | language-skill | Table-driven tests, `t.Parallel()` everywhere. |
| Mocking | `github.com/stretchr/testify/mock`, `go.uber.org/mock/gomock` | language-skill | Mock repository and producer interfaces. |
| Kafka test double | In-process fake producer implementing the internal `EventPublisher` interface | tech-lead-research | Tests never talk to a real broker. |
| PostgreSQL test double | In-memory/repository fakes behind interfaces | tech-lead-research | No testcontainers/Docker in the agent runtime; integration-style tests use fakes that emulate constraints (unique pair, outbox ordering). |
| HTTP test double | `httptest` + Gin test router | language-skill | Handler-level tests with a stubbed auth verifier. |
| Local dev tooling | `github.com/joho/godotenv` (local dev only) | language-skill | Never used in production; `.env` stays out of version control. |

External third-party providers: **none** for this service (no email, payments, voice, or cloud-vendor SDKs). The only "external" cryptographic dependency is identity_service's public verification keys, supplied through configuration.

---

## 3. Container Lifecycle & Probes

- **Statelessness:** never persist state to local disk or process memory across requests. All state in PostgreSQL; publishing via Kafka. Safe under `replicas > 1`.
- **Startup:** load and validate configuration (fail fast with a structured error), run migrations check, open `pgxpool`, initialize Sarama producer, register metrics, then start the HTTP server. `/readyz` returns `503` until the pool and producer are initialized.
- **Liveness `GET /healthz`:** always `200` while the process is responsive; never probes PostgreSQL/Kafka. Returns `503` only during shutdown.
- **Readiness `GET /readyz`:** `200` when the PostgreSQL pool is usable and the Kafka producer is connected; `503` otherwise or once shutdown begins. Unauthenticated.
- **Signals:** trap `SIGTERM` and `SIGINT` via `signal.NotifyContext`. Shutdown sequence: flip readiness → `http.Server.Shutdown(ctx)` with a bounded context (≤ 30 s) → stop accepting new outbox batches, drain the in-flight publish → `producer.Close()` (flushes) → `pool.Close()` → exit 0. Never abandon a committed outbox row that was already read.
- **Health endpoint semantics** are defined in `docs/architecture.md` §6 and must not drift.

---

## 4. Testing Strategy

- **Unit tests (majority):** domain services and repository logic against in-memory fakes. Every domain rule from `docs/user_stories.md` gets a table-driven test: friend-request state machine, already-friends/pending conflicts, self-requests, direct-conversation reuse, group cap ≤ 10, not-friends all-or-nothing, leave/rename participant authorization, version bumping.
- **Handler/integration tests:** Gin test router + fakes for repository, publisher, and the token verifier. Cover status codes, error bodies (`{"error","detail"}`), idempotency-key replay, and cursor pagination stability.
- **Event tests:** assert the full wire shape — envelope (all nine fields, non-empty `trace_id`, correct `schema_ref`, producer = `social_service`) and payload (exact contract schema from `docs/architecture.md`, including `conversation_version` and full participant lists).
- **Determinism & parallelism:** no real network, no real broker, no real database; fixed clocks injected (`Clock` interface) for `occurred_at`/`created_at`; every test function calls `t.Parallel()`; no shared mutable state between tests.
- **No Docker in tests:** the agent runtime has no Docker daemon — use fakes/stubs, never containers, for PostgreSQL, Kafka, or anything else.
- **Coverage:** ≥ 80 % overall with full branch coverage on the domain layer; run with `make test` (includes `-race`).
- **TDD:** write the failing test first for every new behavior and every bug fix (Prove-It pattern); tests are the executable specification of the user stories.

---

## 5. Coding Conventions & Project Structure

Repository layout (Go standard layout, module `social_service`):

```
cmd/server/main.go            # entrypoint: config, wiring, graceful shutdown
internal/
  config/config.go            # typed Config struct, validated at startup
  httpapi/                    # router, middleware (auth, trace, idempotency), handlers
  friendship/                 # friend-request & friendship domain service
  conversation/               # conversation domain service (direct/group, versions)
  repository/                 # pgx repositories + migrations (SQL files)
  events/                     # envelope model, outbox publisher, Kafka producer
  models/                     # domain types and request/response structs
tests/
  unit/                       # domain + repository tests with fakes
  integration/                # handler-level tests over the Gin test router
  mocks/                      # generated/shared mocks and fakes
Makefile  Dockerfile  go.mod  go.sum  .gitignore  README.md
```

Conventions:

- **Layering:** handlers/routes → domain services → repositories. Handlers do binding/validation and error mapping only; domain services hold all business rules and emit events through the outbox; repositories do SQL only. Domain layers never import Gin or Sarama types.
- **Errors:** return `error` values; map to stable `error` codes at the HTTP boundary. Define typed sentinel/domain errors per rule (`ErrSelfRequest`, `ErrAlreadyFriends`, `ErrGroupFull`, …). Never `panic` outside `main` init, never expose stack traces to clients.
- **Interfaces at boundaries:** `Repository`, `EventPublisher`, `TokenVerifier`, `Clock` — this is what makes the service testable without infrastructure.
- **Typing:** explicit request/response structs with validation tags; no anonymous maps in domain code; exported identifiers carry doc comments.
- **SQL safety:** parameterized queries only; the canonical pair (`LEAST`/`GREATEST`) and `direct_pair_key` are computed in the domain layer, then bound as parameters.
- **Comments explain why, not what**; no dead code, no TODOs.

---

## 6. Configuration & Secrets Management

12-factor: all configuration from environment variables, validated at startup (fail fast, structured log, non-zero exit). No `.env` in production (allowed for local dev only, git-ignored).

| Variable | Required | Meaning |
|---|---|---|
| `DATABASE_URL` | yes | PostgreSQL connection string (`postgres://user:pass@host:5432/social?sslmode=...`). |
| `KAFKA_BROKERS` | yes | Comma-separated broker list for `social_events` producing. |
| `KAFKA_CLIENT_ID` | no | Defaults to `social_service`. |
| `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `KAFKA_TLS_ENABLED` | no | SASL/TLS when the deployment provisions them. |
| `TOKEN_VERIFICATION_KEYS_PATH` (or inline `TOKEN_VERIFICATION_KEYS`) | yes | identity_service's public verification keys: current + next, PEM/JSON, for local JWT verification and rotation. |
| `TOKEN_ISSUER`, `TOKEN_AUDIENCE` | yes | Expected `iss` / `aud` claims on access tokens. |
| `HTTP_PORT` | no | Defaults to `8080`. |
| `SHUTDOWN_TIMEOUT_SECONDS` | no | Defaults to `30`. |
| `LOG_LEVEL` | no | `debug|info|warn|error` (default `info`). |

Secrets (DB password, SASL password) arrive as plain env vars injected by the platform or mounted secret files — never in code, never committed. Startup validation prints which variable is missing, never its value.

---

## 7. Observability & Logging Conventions

- **Structured JSON logs** (zap production encoder) with at minimum: `ts`, `level`, `service` (`social_service`), `trace_id`, `operation`, `status`, plus bounded context (entity kinds and counts — never user ids, recipient ids, or conversation names in log fields).
- **Trace propagation:** read `traceparent` / `X-Trace-Id` on every HTTP request; include `trace_id` in every log line; propagate into the event envelope. Mint a fresh 32-hex `trace_id` when a request arrives without one, and at the edge of any flow that has no originating request (never empty).
- **Metrics registration:** all counters/gauges/histograms from `docs/architecture.md` §7 registered at startup via `prometheus/client_golang`; expose `/metrics` (unauthenticated, internal). Labels are low-cardinality (`route` templates, `method`, `status_class`, `action`, `kind`, `event_type`, `topic`) — never user, conversation, or request ids.
- **Mandatory event envelope (this service produces events):** every Kafka message is `{"envelope": {...}, "payload": {...}}` with the envelope carrying exactly `event_id`, `event_type`, `event_version`, `occurred_at`, `producer`, `trace_id`, `correlation_id`, `causation_id`, `schema_ref` — all present, `trace_id` never empty. The envelope is modeled as a single reusable struct/type (never ad-hoc map construction at publish sites, which silently makes `trace_id` optional). `schema_ref` resolves to the contract in the project-level `EVENT_CONTRACTS.md` (`EVENT_CONTRACTS.md#friend_request_changed-v1`, `EVENT_CONTRACTS.md#conversation_changed-v1`). A payload MUST NOT carry an envelope field name at its top level. Envelope fields are also mirrored into Kafka headers (`trace_id`, `event_type`); the JSON envelope remains the source of truth.

---

## 8. Pinned Decisions

| Dependency / Area | Pin / Constraint | Rationale | Added By | Date |
|---|---|---|---|---|
| PostgreSQL driver | `pgx/v5` (not `database/sql` stdlib layer) | Native batch/pipeline support and first-class context handling; platform standard for Go services. | tech-lead | 2026-07-30 |
| Kafka client | Sarama sync producer with idempotent mode | At-least-once with dedupe downstream; single topic, low volume — sync produce keeps outbox semantics simple. | tech-lead | 2026-07-30 |
| User-existence verification | No runtime identity lookup, ever | Keeps the service free of synchronous dependencies by design (architecture §2.4); revisit only with an explicit ADR if product requires eager validation. | tech-lead | 2026-07-30 |

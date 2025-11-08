# Specification: MySQL Binlog Connection & Streaming

## Goal
Establish a resilient MySQL binlog streaming entry point that connects with GTID support, streams row events through existing aggregator/coordinator components, and handles transient failures gracefully.

## User Stories
- As a platform engineer, I want the service to connect to MySQL and continuously stream binlog events so that downstream aggregation receives live configuration changes.
- As an operator, I want connection failures retried with clear logging so that I can diagnose outages quickly without manual intervention.

## Specific Requirements

**Connection Initialization**
- Use `go-mysql` GTID-based `Canal`/`Replication` APIs with ROW format assumed enabled on the server.
- Load host, user, password from Vault-generated JSON file; port fixed at 3306.
- Avoid DSN strings; log database identifier only (no credentials).
- Initialize logging and metrics before establishing the connection.

**Credential & Config Loading**
- Parse Vault JSON once at startup; surface configuration errors with actionable logs.
- Support environment variable override for the Vault JSON path if necessary.
- Validate required fields (host, user, password) and fail fast on missing data.

**Retry & Backoff Handling**
- On initial connection or subsequent errors, apply exponential backoff with jitter capped near 3s.
- Log each retry attempt with error details; stop only on fatal/non-retryable errors.
- Respect process `context.Context` cancellation to abort retries cleanly.

**GTID Tracking**
- Maintain in-memory GTID position during runtime and reuse on reconnects.
- If no GTID captured (cold start), fall back to current master position.
- Defer durable GTID persistence to future roadmap items.

**Event Streaming Pipeline**
- Filter out events from `METADATA_TABLE` and tables lacking the `CONFIG_DATA_TABLE_PREFIX` prefix from `mars-lib`.
- Map remaining row events directly into existing aggregator pipeline (`StreamEvent` with namespace/pop).
- Reuse or extend types and helpers in `internal/binlog` to avoid duplicate logic.

**Metrics & Logging**
- Increment connection/stream error counters and processed row counters via existing observability APIs.
- Emit concise info/warn/error logs for connection state, retries, and fatal shutdowns.
- Ensure logging excludes sensitive data and references only the single configured database.

**Application Entry Point**
- Provide `main.go` that boots logging/metrics, loads configuration, creates the binlog streamer, coordinator, and aggregator, and starts streaming.
- Handle OS signals for graceful shutdown (cancel context, close channels).
- Exit with non-zero status on unrecoverable errors during startup or streaming.

## Visual Design
No visual assets provided.

## Existing Code to Leverage

**`internal/binlog/streamer.go` (streaming logic)**
- Implements the existing streaming state machine; integrate connection setup here rather than duplicating loops.
- Reuse helper functions for table filtering, row mapping, and metric increments.

**`internal/aggregator` package**
- `Coordinator` and `Aggregator` already consume events emitted by `streamer.go`; wire them directly in the entry point.
- Honor existing logging conventions (`slog` cached logger) and batching semantics.

**`internal/observability/metrics.go`**
- Record connection errors and processed row counters using exported observability functions.

## Out of Scope
- TLS, IAM authentication, or multi-source replication.
- Durable GTID persistence across restarts or checkpointing to external storage.
- Advanced schema/table filters beyond prefix-based inclusion and metadata exclusion.
- Kafka publishing, GTID topic updates, or downstream batching modifications (covered by other roadmap items).


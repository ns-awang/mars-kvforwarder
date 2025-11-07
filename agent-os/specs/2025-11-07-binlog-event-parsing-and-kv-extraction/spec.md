# Specification: Binlog Event Parsing & KV Extraction

## Goal
Parse MySQL ROW binlog events from `config_data_{namespace}_{POP}` tables and emit per-row KV changes with `TxID`, `Namespace`, and `Pop`, preserving transaction semantics, last‑write‑wins per key, and passing through values unmodified.

## User Stories
- As a platform engineer, I need each binlog row to become a KV change so downstream batching can flush atomic messages.
- As a consumer service, I want each KV change annotated with `namespace` and `pop` so I can route and apply updates correctly.

## Specific Requirements

**Parsing**
- Process MySQL binlog in ROW format; support `INSERT`, `UPDATE` (post‑image), and `DELETE`.
- For a multi‑row RowEvent, emit a separate row event per row (row‑by‑row).
- Extract `TxID` (transaction identifier) for each row; attach to emitted events.

**Table Convention & Extraction**
- Only handle tables named with prefix `CONFIG_DATA_TABLE_PREFIX` (e.g., `config_data_`), followed by `{namespace}_{POP}`.
- Derive `namespace` and `pop` from the table name (split on `_` after the configured prefix).
- Ignore the `tenant` column; it does not influence key/value.

**Key/Value Mapping**
- Key = `config_key` (varchar). Must be non‑NULL; skip rows with NULL key, log error.
- Value = raw bytes from `config_value` (mediumblob). Do not transform or re‑encode; NULL is allowed and must pass through as NULL.
- Operation types: `create` for INSERT, `update` for UPDATE (post‑image), `delete` for DELETE.

**Emission**
- Emit a per‑row `StreamEvent` carrying: `TxID`, `Namespace`, `Pop`, and `KVChange{Key []byte|string, Value []byte (pass‑through), Operation}`.
- Intra‑batch order is not required; uniqueness (last‑write‑wins per key) is enforced by downstream Aggregator.

**Error Handling & Retries**
- On row parse or mapping failure: log an error with context (table, txid, namespace, pop, operation) and increment a metric.
- Retry up to 3 attempts for a transient parse/mapping error. If all attempts fail, stop the stream and surface the failure.

**Interfaces & Integration**
- Extend `internal/aggregator/pipeline.go` `StreamEvent` to include `Namespace string` and `Pop string` for `RowChange` events (do not impact `TxnBegin`/`TxnCommit`).
- Keep `KVChange.Value` as `[]byte` and unchanged by the parser.
- Ensure emitted events align with existing `Coordinator.Run(ctx)` model (bounded channels, backpressure).

**Metrics & Logging**
- Metrics: per‑row processed counter; parse/mapping error counter; retry counter; fatal stop counter.
- Logging: error logs include table name, txid, namespace, pop, operation, and a compact reason.
- Reuse mars‑lib logger (`GetLogger()`); avoid verbose log spam.

## Visual Design
No visuals.

## Existing Code to Leverage
- `internal/aggregator` (Aggregator, headers hashing, coordinator/backpressure) — downstream batching remains unchanged.
- `internal/aggregator/pipeline.go` — extend `StreamEvent` to carry `Namespace` and `Pop` for row changes.
- `github.com/netSkope/mars-lib` — use common constants (e.g., `CONFIG_DATA_TABLE_PREFIX`) and logger.

## Out of Scope
- Kafka topic selection/routing (handled in another roadmap task).
- Exactly‑once delivery semantics; we provide at‑least‑once.
- Any transformation of `config_value`; it is pass‑through.


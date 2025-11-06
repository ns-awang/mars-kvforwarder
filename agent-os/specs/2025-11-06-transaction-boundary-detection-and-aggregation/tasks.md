# Tasks: Transaction Boundary Detection & Aggregation

- [x] Task Group 1: Aggregation Semantics & In-Memory Model
  - [x] Implement per-transaction buffer with last-write-wins per key
  - [x] Track GTID/tx-id and commit-order alignment
  - [x] Collapse multiple row ops per key within a transaction
  - [x] Unit tests for aggregation semantics (last-write-wins, commit-order)

 - [x] Task Group 2: Batching & Flush Policy
  - [x] Flush when 50 KV updates reached or COMMIT observed
  - [x] Split large transactions across multiple messages with batch index/total
  - [x] Derive deterministic idempotency key (e.g., GTID + batch index)
  - [x] Unit tests for flush conditions, transaction splitting, idempotency keying

 - [x] Task Group 3: Headers & Integrity
  - [x] Add SHA256 payload checksum header
  - [x] Add previous and current txid headers
  - [x] Add batch index and batch total headers
  - [x] Unit tests for header presence, values, and checksum correctness

 - [x] Task Group 4: Backpressure & Concurrency
  - [x] Configure fixed-size input and output channel buffers
  - [x] Ensure blocking behavior when producer lags; avoid unbounded memory
  - [x] Unit tests for bounded queues and blocking behavior

 - [x] Task Group 5: Metrics & Logging
  - [x] Emit latency, throughput, and error metrics
  - [x] Structured logs for flush events with txid/GTID and batch info
  - [x] Unit tests/observability checks for metric emission and key log events

- [ ] Task Group 6: Interfaces & Integration Points (cancelled)
  - Cancelled per YAGNI; deferring interface and config abstractions until needed

- [x] Task Group 7: Spec Conformance Review & Test Completeness
  - [x] Review implementation against spec and requirements summary
  - [x] Identify and add any missing unit tests across all task groups
  - [x] Ensure critical paths covered (aggregation, batching, headers, backpressure)



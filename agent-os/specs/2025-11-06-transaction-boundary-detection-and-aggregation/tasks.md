# Tasks: Transaction Boundary Detection & Aggregation

- [x] Task Group 1: Aggregation Semantics & In-Memory Model
  - [x] Implement per-transaction buffer with last-write-wins per key
  - [x] Track GTID/tx-id and commit-order alignment
  - [x] Collapse multiple row ops per key within a transaction
  - [x] Unit tests for aggregation semantics (last-write-wins, commit-order)

- [ ] Task Group 2: Batching & Flush Policy
  - [ ] Flush when 50 KV updates reached or COMMIT observed
  - [ ] Split large transactions across multiple messages with batch index/total
  - [ ] Derive deterministic idempotency key (e.g., GTID + batch index)
  - [ ] Unit tests for flush conditions, transaction splitting, idempotency keying

- [ ] Task Group 3: Headers & Integrity
  - [ ] Add SHA256 payload checksum header
  - [ ] Add previous and current txid headers
  - [ ] Add batch index and batch total headers
  - [ ] Unit tests for header presence, values, and checksum correctness

- [ ] Task Group 4: Backpressure & Concurrency
  - [ ] Configure fixed-size input and output channel buffers
  - [ ] Ensure blocking behavior when producer lags; avoid unbounded memory
  - [ ] Unit tests for bounded queues and blocking behavior

- [ ] Task Group 5: Metrics & Logging
  - [ ] Emit latency, throughput, and error metrics
  - [ ] Structured logs for flush events with txid/GTID and batch info
  - [ ] Unit tests/observability checks for metric emission and key log events

- [ ] Task Group 6: Interfaces & Integration Points
  - [ ] Define input record and transaction event interfaces from streaming component
  - [ ] Define output message builder interface for Kafka (confluent-kafka-go)
  - [ ] Add configuration knobs (channel sizes, batch size, topic names)
  - [ ] Unit tests for interface contracts and configuration validation

- [ ] Task Group 7: Spec Conformance Review & Test Completeness
  - [ ] Review implementation against spec and requirements summary
  - [ ] Identify and add any missing unit tests across all task groups
  - [ ] Ensure critical paths covered (aggregation, batching, headers, backpressure)



# Specification: Transaction Boundary Detection & Aggregation

## Goal
Aggregate and buffer KV updates from the binlog stream, preserving transaction semantics and ordering, and flush messages to Kafka when either 50 KV updates are accumulated or a COMMIT is observed.

## User Stories
- As a system operator, I want transaction-aware batching so consumers receive consistent, ordered updates.
- As a downstream service, I want headers that let me reconstruct transaction boundaries and verify payload integrity.

## Specific Requirements

**Aggregation Semantics**
- Collapse multiple changes to the same key within a single transaction using last-write-wins.
- Maintain commit-order emission aligned with GTID ordering.
- Receive only meaningful records (upstream filtering handled by streaming component).

**Batching & Flush Policy**
- Flush a data message when either condition is met: (a) 50 KV updates accumulated, (b) COMMIT event observed.
- If a single transaction exceeds 50 KV updates, split across multiple messages while preserving transaction grouping via headers (batch index/total).
- Do not require waiting for COMMIT to flush once 50 KV is reached.

**Headers & Integrity**
- Include SHA256 of the payload in message headers.
- Include previous txid and current txid in headers to form a verifiable chain.
- Include batch index and batch total in headers for multi-message transactions.

**Backpressure & Concurrency**
- Use fixed-size channel buffers for incoming stream records and for generated Kafka messages.
- Block the aggregator when the producer cannot keep up; no unbounded buffering.

**Delivery & Idempotency**
- At-least-once delivery with retries to Kafka.
- Deterministic idempotency key derivation (e.g., GTID + batch index) so consumers can dedupe.

**Metrics & Observability**
- Record latency, throughput, and error counts.
- Basic structured logs for flush events with txid/GTID and batch info.

## Visual Design
No visual assets provided.

## Existing Code to Leverage
No reusable components identified in the current codebase for this feature.

## Out of Scope
- Upstream binlog parsing and filtering logic.
- Exactly-once semantics via Kafka transactions.
- Additional observability beyond latency, throughput, and error metrics.


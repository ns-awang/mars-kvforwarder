# Spec Requirements: transaction-boundary-detection-and-aggregation

## Initial Description
Work on the 3rd item in the roadmap first -- Transaction Boundary Detection & Aggregation. In this spec, we focus on how to buffer and aggregate the kv pairs received from streaming the binlog.

## Requirements Discussion

### First Round Questions

**Q1:** I assume we should collapse multiple changes to the same key within a single transaction into the final state (last-write-wins) before emitting. Is that correct, or do you want to preserve the full sequence of per-row ops within the transaction?
**Answer:** correct

**Q2:** For batching, I’m assuming we flush exactly at COMMIT, even if a transaction contains more than 50 KV updates, meaning we’d send multiple messages but keep a transaction grouping via headers (e.g., tx-id, batch index/total). Is that correct, or should the “at-most 50 KV per message” rule override and split transactions without explicit transaction grouping?
**Answer:** keep transaction grouping in the header; do not need to wait for COMMIT to flush—flush when either 50 KV reached or COMMIT seen.

**Q3:** I’m assuming we’ll enforce backpressure limits: a maximum number of in-flight/active transactions buffered and a maximum memory/record count per transaction (spill/abort strategy for very large/long-running transactions). Is that acceptable, or should we allow unbounded buffering per transaction?
**Answer:** correct; assume fixed-size channel buffers for stream input and for generated Kafka messages; aggregator blocks if producer cannot keep up.

**Q4:** For ordering guarantees, I’m assuming a single MySQL source stream with global ordering preserved by GTID, and we will emit messages in commit order only. Is that correct, or do we need per-table/per-key ordering semantics that differ from commit order?
**Answer:** correct

**Q5:** Please confirm the table/schema filter and KV mapping: which tables are included, what forms the key (primary key, composite), and which columns comprise the value payload. Should this be driven by configuration (per-table mapping), or is there a single uniform mapping?
**Answer:** all tables included; upstream streaming logic filters out uninterested records so the aggregator only receives meaningful ones.

**Q6:** Error handling and idempotency: I’m assuming at-least-once delivery to Kafka with retries, and idempotency based on a deterministic key (e.g., GTID plus batch index) so consumers can dedupe. Is that correct, or do you prefer exactly-once semantics via Kafka transactions?
**Answer:** correct

**Q7:** Headers beyond SHA256 and prev/current GTID: do you want to include tx-id (or GTID as tx identifier), commit timestamp, batch index/total, and a per-transaction message sequence to allow consumers to reconstruct the transaction? If so, which of these are required?
**Answer:** include SHA256, previous txid, current txid, and batch index/total.

**Q8:** Observability: I propose metrics for buffered transactions, records per transaction, flush latency, flush count, spills, and retry counts; plus structured logs on BEGIN/COMMIT/flush with tx-id/GTID. Is that sufficient, or should we add trace propagation or custom audit fields?
**Answer:** only record latency, throughput, and error for now.

### Existing Code to Reference
No similar existing features identified for reference.

### Follow-up Questions
None at this time.

## Visual Assets

### Files Provided:
No visual assets provided.

## Requirements Summary

### Functional Requirements
- Buffer and aggregate KV pair changes from the binlog stream.
- Collapse multiple changes to the same key within a transaction (last-write-wins).
- Flush a data message when either: (a) 50 KV updates accumulated, or (b) a COMMIT event is observed.
- Preserve transaction grouping across messages using headers (include batch index/total for multi-message transactions).
- Maintain commit-order emission aligned with GTID ordering.
- Include message headers: SHA256 of payload, previous txid, current txid, batch index/total.
- Provide at-least-once delivery semantics with deterministic idempotency keys (e.g., GTID + batch index).

### Reusability Opportunities
- None identified.

### Scope Boundaries
**In Scope:**
- Aggregation/buffering logic, batching policy, and transaction grouping via headers.
- Backpressure via fixed-size channels; blocking behavior when producer lags.
- Metrics for latency, throughput, and errors.

**Out of Scope:**
- Upstream filtering/mapping from raw binlog events (assumed handled by streaming component).
- Exactly-once delivery via Kafka transactions.
- Additional observability (tracing, audit fields) beyond specified metrics.

### Technical Considerations
- Tech stack: Go, confluent-kafka-go client; GTID-based ordering; Kafka data topic + compacted topic for positions.
- Integration points: Input from binlog streaming component (pre-filtered); output to Kafka producers (data and GTID topics).
- Constraints: At-most 50 KV per message; transaction grouping maintained across splits; memory bounded by channel sizes.
- Idempotency: Use deterministic keys (GTID + batch index) for consumer dedupe; at-least-once producer retries.



# Spec Requirements: Binlog Event Parsing & KV Extraction

## Initial Description
Work on the 2nd roadmap item — Binlog Event Parsing & KV Extraction. This is core logic to parse MySQL binlog row events and emit per-row KV changes for downstream processing.

## Requirements Discussion

### First Round Questions

**Q1:** I’m assuming MySQL binlog format is row-based (ROW) and we need INSERT/UPDATE/DELETE row images. Is that correct?
**Answer:** correct

**Q2:** For the KV key, I’m assuming it’s derived from the table’s primary key (or a configured composite) serialized to bytes. Is that correct, or do we need a mapping per table?
**Answer:** table format:
`config_data_{namespace}_{POP}(config_key varchar(255) PK, config_value mediumblob, tenant varchar(16))`. Ignore `tenant`. Extract `namespace` and `POP` from table name and pass them down to Aggregator.

**Q3:** KVForwarder should not modify the value stream from MySQL. OK to pass raw bytes?
**Answer:** KVForwarder should not modify the value; pass-through as is.

**Q4:** For UPDATE, emit full post-image rather than diffs?
**Answer:** correct

**Q5:** Handling NULLs/limits?
**Answer:** `config_key` should not be NULL. `config_value` may be NULL and must be passed as NULL. Column limits follow schema.

**Q6:** Should namespace and POP be included for consumers?
**Answer:** namespace and POP must be included; target Kafka topic decided elsewhere.

**Q7:** Error handling and retries?
**Answer:** log error, increment metric, retry up to 3 times, stop the stream if all attempts fail.

**Q8:** RowEvent with multiple rows—how to emit?
**Answer:** pass on row-by-row; split multi-row RowEvents into individual row messages; each must include transaction id, namespace, and POP.

**Q9:** Interface consistency with pipeline/aggregator?
**Answer:** ensure `StreamEvent` carries per-row payload and includes `TxID`, `Namespace`, and `Pop`. Aggregator should receive per-row `KVChange` with key/value/op and be able to include `Namespace` and `Pop` in batch (e.g., headers).

### Existing Code to Reference
Current `internal/aggregator/pipeline.go` defines `StreamEvent{Type, TxID, Change}`; update to include `Namespace` and `Pop` for per-row events. `KVChange` remains key/value/op (value is []byte, unchanged). Batch headers already support tx-related metadata; add namespace/pop headers for batches derived from a single table.

### Follow-up Questions
None at this time.

## Visual Assets
No visual assets provided.

## Requirements Summary

### Functional Requirements
- Parse MySQL binlog ROW format for INSERT/UPDATE/DELETE.
- Table convention: `config_data_{namespace}_{POP}`; ignore `tenant` column; key = `config_key` (varchar), value = raw `config_value` (mediumblob).
- Extract `namespace` and `POP` from table name per row; include them with each emitted row.
- For UPDATE, use post-image as the value; do not modify value bytes.
- Split multi-row RowEvents into individual row `StreamEvent`s.
- Emit per-row events with `TxID`, `Namespace`, `Pop`, and KV {key, value, operation}.

### Error Handling
- On parse/mapping failure: log error, increment metric, retry up to 3 times; if still failing, stop the stream.

### Interface Notes
- Extend `StreamEvent` to include `Namespace string` and `Pop string` (for RowChange events).
- Aggregator may include `Namespace`/`Pop` in batch headers so downstream consumers can route/process accordingly.

### Observability
- Metrics: parse failures, retries, fatal stop; per-row processed counter.
- Logs: errors with context (table, namespace, pop, txid), retry attempts, and final stop reason.


# Tasks: Binlog Event Parsing & KV Extraction

- [ ] Task Group 1: Connector & Session Configuration (cancelled)
  - Assumption: connection and binlog settings are already provisioned per roadmap item 1

- [ ] Task Group 2: Event Fetch Loop & Reconnect/Backoff
  - [ ] Implement main fetch loop for binlog events
  - [ ] Reconnect with exponential backoff on transient failures
  - [ ] Unit tests for reconnect/backoff behavior

- [ ] Task Group 3: Transaction Demarcation & TxID Extraction
  - [ ] Detect BEGIN/COMMIT and extract TxID for each row
  - [ ] Attach TxID to emitted per-row events
  - [ ] Unit tests for demarcation and TxID tagging

- [ ] Task Group 4: Table Name Parsing (namespace/pop)
  - [ ] Use CONFIG_DATA_TABLE_PREFIX (e.g., `config_data_`) to extract `{namespace}_{POP}`
  - [ ] Validate and handle malformed names (metric + error log)
  - [ ] Unit tests for happy-path and malformed table names

- [ ] Task Group 5: Row Mapping to KV
  - [ ] Key = `config_key` (non-NULL); skip+error log if NULL
  - [ ] Value = raw `config_value` bytes; pass through NULL
  - [ ] Operation mapping: INSERT=create, UPDATE=update (post-image), DELETE=delete
  - [ ] Unit tests for key/value mapping and NULL/edge cases

- [ ] Task Group 6: RowEvent Splitting & Emission
  - [ ] Split multi-row RowEvents into per-row `StreamEvent`
  - [ ] Emit `TxID`, `Namespace`, `Pop`, and `KVChange{Key, Value, Operation}`
  - [ ] Ensure alignment with `Coordinator.Run(ctx)` and channels/backpressure
  - [ ] Unit tests for multi-row splitting and event fields

- [ ] Task Group 7: Error Handling & Observability
  - [ ] Error logging with context (table, txid, namespace, pop, op)
  - [ ] Metrics: processed rows, parse errors, retries, fatal stops
  - [ ] Unit tests for metrics increments and error logs (assert via fakes)

- [ ] Task Group 8: Pipeline Interface Consistency
  - [ ] Extend `StreamEvent` to include `Namespace` and `Pop` (RowChange only)
  - [ ] Verify aggregator receives per-row KV unchanged (value pass-through)
  - [ ] Unit tests to validate event struct compatibility and aggregator handshake

- [ ] Task Group 9: Production Review & Completeness
  - [ ] Review code for performance (allocations, logging volume) and failure modes
  - [ ] Add any missing unit tests for uncovered behaviors
  - [ ] Verify all spec requirements are covered and documented


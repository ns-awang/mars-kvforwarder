# Tasks: Binlog Event Parsing & KV Extraction

- [ ] Task Group 1: Connector & Session Configuration (cancelled)
  - Assumption: connection and binlog settings are already provisioned per roadmap item 1

- [x] Task Group 2: Event Fetch Loop & Reconnect/Backoff
  - [x] Implement main fetch loop for binlog events
  - [x] Reconnect with exponential backoff on transient failures
  - [x] Unit tests for reconnect/backoff behavior

- [x] Task Group 3: Transaction Demarcation & TxID Extraction
  - [x] Detect BEGIN/COMMIT and extract TxID for each row
  - [x] Attach TxID to emitted per-row events
  - [x] Unit tests for demarcation and TxID tagging

- [x] Task Group 4: Table Name Parsing (namespace/pop)
  - [x] Use CONFIG_DATA_TABLE_PREFIX (e.g., `config_data_`) to extract `{namespace}_{POP}`
  - [x] Validate and handle malformed names (metric + error log)
  - [x] Unit tests for happy-path and malformed table names

- [x] Task Group 5: Row Mapping to KV
  - [x] Key = `config_key` (non-NULL); skip+error log if NULL
  - [x] Value = raw `config_value` bytes; pass through NULL
  - [x] Operation mapping: INSERT=create, UPDATE=update (post-image), DELETE=delete
  - [x] Unit tests for key/value mapping and NULL/edge cases

- [x] Task Group 6: RowEvent Splitting & Emission
  - [x] Split multi-row RowEvents into per-row `StreamEvent`
  - [x] Emit `TxID`, `Namespace`, `Pop`, and `KVChange{Key, Value, Operation}`
  - [x] Ensure alignment with `Coordinator.Run(ctx)` and channels/backpressure
  - [x] Unit tests for multi-row splitting and event fields

- [x] Task Group 7: Error Handling & Observability
  - [x] Error logging with context (table, txid, op)
  - [x] Metrics: processed rows, parse errors
  - [x] Unit tests for metrics increments

- [ ] Task Group 8: Pipeline Interface Consistency
  - [ ] Extend `StreamEvent` to include `Namespace` and `Pop` (RowChange only)
  - [ ] Verify aggregator receives per-row KV unchanged (value pass-through)
  - [ ] Unit tests to validate event struct compatibility and aggregator handshake

- [ ] Task Group 9: Production Review & Completeness
  - [ ] Review code for performance (allocations, logging volume) and failure modes
  - [ ] Add any missing unit tests for uncovered behaviors
  - [ ] Verify all spec requirements are covered and documented


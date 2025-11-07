# Product Roadmap

1. [ ] MySQL Binlog Connection & Streaming — Establish connection to MySQL server, configure binlog streaming with GTID support, and implement basic event reception loop with error handling and reconnection logic `M`

2. [ ] Binlog Event Parsing & KV Extraction — Parse binlog row change events (INSERT, UPDATE, DELETE), extract key-value pairs from table rows, identify operation type (create/update/delete), and handle different MySQL data types `M`

3. [x] Transaction Boundary Detection & Aggregation — Track transaction start/commit events, aggregate KV changes with last-write-wins per key, flush on 50 unique keys or COMMIT, and ensure atomic batches per transaction window `M`

4. [ ] Kafka Data Topic Producer — Implement Kafka producer for data messages, batch up to 50 KV pair updates per message, ensure one complete transaction per message when transaction boundary is reached, and handle Kafka producer errors and retries `M`

5. [ ] GTID Position Tracking in Compacted Topic — Create Kafka producer for compacted GTID tracking topic, record GTID position after each successfully published data message batch, and implement idempotent GTID updates `S`

6. [ ] Startup Recovery & GTID Resume — On application startup, fetch last recorded GTID from compacted Kafka topic, connect to MySQL binlog starting from that GTID position, and handle cases where GTID is not found (start from beginning) `S`

7. [ ] Message Header Integrity (SHA256) — Calculate SHA256 checksum of message payload, include checksum in Kafka message header for each data message, and provide utilities for downstream consumers to verify integrity `S`

8. [ ] GTID Chain Tracking in Headers — Track previous GTID for each message, include both previous GTID and current GTID in Kafka message headers, and maintain GTID chain continuity across message boundaries `S`

> Notes
> - Order items by technical dependencies and product architecture
> - Each item should represent an end-to-end (frontend + backend) functional and testable feature


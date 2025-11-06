# Product Mission

## Pitch
KVForwarder is a data streaming service that helps the MARS system distribute key-value pair data from AWS to private cloud datacenters worldwide by providing reliable, ordered, and transaction-aware replication of MySQL binlog changes to Kafka.

## Users

### Primary Customers
- **MARS System Operators**: Internal teams responsible for maintaining and operating the MARS data distribution system
- **MARS System Consumers**: Internal teams that rely on the MARS system to receive distributed key-value data in their private cloud datacenters

### User Personas
**System Operator** (25-45)
- **Role:** DevOps Engineer / Platform Engineer
- **Context:** Responsible for maintaining data replication infrastructure across AWS and global private cloud datacenters
- **Pain Points:** Ensuring data consistency, handling system restarts gracefully, maintaining transaction integrity during replication
- **Goals:** Reliable, fault-tolerant data streaming with automatic recovery and clear observability

**Data Consumer** (25-45)
- **Role:** Backend Engineer / Data Engineer
- **Context:** Building services that consume distributed key-value data in private cloud environments
- **Pain Points:** Receiving out-of-order data, missing transaction boundaries, data integrity concerns
- **Goals:** Guaranteed ordering, transaction-aware data delivery, and data integrity verification

## The Problem

### Distributed Data Replication Complexity
Replicating MySQL data changes from AWS to multiple private cloud datacenters worldwide requires maintaining strict ordering, transaction boundaries, and data integrity. Traditional replication solutions often lose transaction context, fail to preserve ordering guarantees, and lack robust recovery mechanisms. This results in data inconsistencies, application errors, and manual intervention during failures.

**Our Solution:** KVForwarder streams MySQL binlog changes, aggregates them into key-value pairs while preserving transaction boundaries and sequential ordering, and reliably delivers them to Kafka with built-in recovery mechanisms and data integrity checks.

## Differentiators

### Transaction-Aware Streaming
Unlike generic binlog replication tools, KVForwarder maintains complete transaction context by grouping changes within transaction boundaries and including transaction metadata in every message. This results in downstream consumers receiving complete, atomic transaction updates.

### Guaranteed Ordering with Recovery
Unlike stateless replication tools, KVForwarder tracks binlog position (GTID) in a Kafka compacted topic and automatically resumes from the last committed position on restart. This results in zero data loss and guaranteed sequential processing even after failures.

### Built-in Data Integrity
Unlike basic message producers, KVForwarder includes SHA256 checksums and GTID tracking in every message header. This results in downstream systems being able to verify data integrity and maintain precise replication state.

## Key Features

### Core Features
- **Binlog Streaming & Parsing:** Continuously streams MySQL binlog events, parses row changes, and aggregates them into key-value pairs with operation metadata (create/update/delete)
- **Transaction Preservation:** Maintains transaction boundaries and sequential ordering, ensuring downstream systems receive data in the exact order it was committed
- **Kafka Message Batching:** Intelligently batches up to 50 KV pair updates or one complete MySQL transaction per Kafka message for optimal throughput and transaction atomicity

### Collaboration Features
- **GTID Position Tracking:** Records binlog position (GTID) in a Kafka compacted topic, enabling precise state management and coordination across system restarts
- **Automatic Recovery:** On restart, automatically fetches the last recorded GTID and resumes streaming from the exact point of interruption, ensuring zero data loss

### Advanced Features
- **Message Integrity Verification:** Includes SHA256 checksum of the payload in every message header, enabling downstream systems to verify data integrity
- **GTID Chain Tracking:** Includes both previous and current GTID in message headers, creating a verifiable chain of replication state for audit and recovery purposes


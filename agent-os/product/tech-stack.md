# Product Tech Stack

## Framework & Runtime
- **Application Framework:** Gin
- **Language/Runtime:** Go (Golang only - Python3 is not used in this component)

## Database & Storage
- **Database:** MySQL (source database for binlog streaming)
- **Message Queue:** Kafka (for data distribution and GTID position tracking)

## Deployment & Infrastructure
- **Hosting:** AWS, Kubernetes (K8S)
- **CI/CD:** Drone, Helm, Spinnaker

## Third-Party Services & Libraries
- **Monitoring:** Prometheus
- **MySQL Binlog Library:** Go library for MySQL binlog replication (e.g., go-mysql, or similar)
- **Kafka Client:** confluent-kafka-go

## Key Technical Components
- **Binlog Replication:** MySQL binary log streaming with GTID support
- **Kafka Producers:** For publishing data messages and GTID position updates
- **Kafka Compacted Topics:** For GTID position state management
- **Cryptographic Hashing:** SHA256 for message integrity verification


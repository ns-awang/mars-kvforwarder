# Task Breakdown: MySQL Binlog Connection & Streaming

## Overview
Total Task Groups: 6

## Task List

### Configuration & Secrets Setup

#### Task Group 1: Vault JSON Loading & Configuration
**Dependencies:** None

- [ ] 1.0 Load configuration from Vault JSON
  - [ ] 1.1 Tests (2-4) verifying JSON parsing, missing field errors, and default fallbacks
  - [ ] 1.2 Implement config loader to read host/user/password (port fixed 3306)
  - [ ] 1.3 Validate required fields; return actionable errors
  - [ ] 1.4 Log database identifier (no credentials) on successful load
  - [ ] 1.5 Run only tests from 1.1

**Acceptance Criteria:**
- Config loader returns structured settings with host/user/password
- Missing or invalid JSON produces clear errors
- Logging excludes secrets
- Tests in 1.1 pass

### Binlog Connection & Stream Bootstrap

#### Task Group 2: Connection Factory & Retry Logic
**Dependencies:** Task Group 1

- [ ] 2.0 Establish MySQL binlog connection with exponential backoff
  - [ ] 2.1 Tests (3-5) covering retry backoff behavior, fatal error exit, and context cancellation
  - [ ] 2.2 Implement connector using go-mysql with GTID enabled and ROW format assumed
  - [ ] 2.3 Integrate exponential backoff (reusing `internal/binlog` helpers where possible)
  - [ ] 2.4 Ensure logs identify database only; no DSN output
  - [ ] 2.5 Run only tests from 2.1

**Acceptance Criteria:**
- Connector retries transient failures with capped backoff
- Fatal errors bubble up with log entry
- Context cancellation stops retry loop
- Tests in 2.1 pass

#### Task Group 3: GTID Session Tracking
**Dependencies:** Task Group 2

- [ ] 3.0 Manage in-memory GTID state for reconnects
  - [ ] 3.1 Tests (2-4) validating GTID capture, reuse on reconnect, and fallback to master when absent
  - [ ] 3.2 Hook into streamer state to capture latest GTID
  - [ ] 3.3 On reconnect, start from captured GTID or master if empty
  - [ ] 3.4 Document GTID limitations in code comments per standards
  - [ ] 3.5 Run only tests from 3.1

**Acceptance Criteria:**
- GTID stored in-memory updates as events stream
- Reconnect attempts use last GTID when available
- Fresh start uses master position
- Tests in 3.1 pass

### Streamer & Aggregator Integration

#### Task Group 4: Stream Pipeline Wiring
**Dependencies:** Task Groups 2 & 3

- [ ] 4.0 Wire streamer output to aggregator Coordinator
  - [ ] 4.1 Tests (3-5) covering end-to-end event emission with filters applied (metadata exclusion, prefix inclusion)
  - [ ] 4.2 Reuse existing `internal/binlog/streamer.go`; add connection bootstrap entry point
  - [ ] 4.3 Ensure filtering excludes `METADATA_TABLE` and non-prefixed tables
  - [ ] 4.4 Confirm metrics/logging integration with observability package
  - [ ] 4.5 Run only tests from 4.1

**Acceptance Criteria:**
- Streamer receives events and forwards to Coordinator without new abstractions
- Filtering rules match spec (metadata excluded, CONFIG_DATA tables only)
- Metrics increment for processed rows and parse errors
- Tests in 4.1 pass

#### Task Group 5: Application Entry Point (`main.go`)
**Dependencies:** Task Groups 1-4

- [ ] 5.0 Implement service startup and shutdown flow
  - [ ] 5.1 Tests (2-4) using integration-style harness or mocks to validate boot errors and graceful shutdown
  - [ ] 5.2 Initialize logging, metrics, configuration, connector, streamer, and aggregator
  - [ ] 5.3 Handle OS signals (SIGINT/SIGTERM) via context cancellation
  - [ ] 5.4 Exit with non-zero status on unrecoverable startup failures
  - [ ] 5.5 Run only tests from 5.1

**Acceptance Criteria:**
- `main.go` boots all components and begins streaming
- Signal interrupt gracefully stops streaming and closes resources
- Startup failures produce actionable logs and non-zero exit
- Tests in 5.1 pass

### Verification & Documentation

#### Task Group 6: Spec Compliance & Documentation Sync
**Dependencies:** Task Groups 1-5

- [ ] 6.0 Verify completeness and documentation updates
  - [ ] 6.1 Review all tests from groups 1-5 and add up to 4 additional tests if gaps found
  - [ ] 6.2 Confirm metrics/logging align with spec and standards
  - [ ] 6.3 Ensure spec.md and requirements.md accurately reflect final implementation
  - [ ] 6.4 Run the combined feature-specific test suite (tests from groups 1-6)

**Acceptance Criteria:**
- All feature-specific tests pass
- Documentation and spec remain in sync with code
- Observability/logging coverage confirmed
- No outstanding gaps vs. spec requirements

## Execution Order
1. Task Group 1: Vault JSON Loading & Configuration
2. Task Group 2: Connection Factory & Retry Logic
3. Task Group 3: GTID Session Tracking
4. Task Group 4: Stream Pipeline Wiring
5. Task Group 5: Application Entry Point
6. Task Group 6: Spec Compliance & Documentation Sync


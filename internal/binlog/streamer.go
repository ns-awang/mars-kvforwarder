package binlog

import (
	"context"
	"strings"
	"time"

	obs "mars-kvforwarder/internal/observability"

	"github.com/go-mysql-org/go-mysql/replication"
	mlog "github.com/netSkope/mars-lib/src/log"
)

// RowEvent represents a single row parsed from the binlog after demarcation.
// Only TxID is modeled here for now; row payload mapping is handled elsewhere.
type RowEvent struct{ TxID string }

// BinlogEventSource is satisfied by *replication.BinlogStreamer; extracted for testability.
type BinlogEventSource interface {
	GetEvent(ctx context.Context) (*replication.BinlogEvent, error)
}

// TransientError marks an error as retryable.
type TransientError struct{ Err error }

func (e TransientError) Error() string { return e.Err.Error() }

// StreamBinlog consumes events from src and invokes sink for each event.
// Retries transient errors up to retryLimit with exponential backoff; stops on fatal errors.
func StreamBinlog(ctx context.Context, src BinlogEventSource, sink func(RowEvent)) error {
	const (
		retryLimit     = 3
		initialBackoff = 100 * time.Millisecond
		maxBackoff     = 3 * time.Second
	)
	log := mlog.GetLogger()
	backoff := initialBackoff
	retries := 0
	currentTxID := ""
	for {
		if ctx.Err() != nil {
			return nil
		}

		ev, err := src.GetEvent(ctx)
		if err != nil {
			// non-transient → early return
			if _, ok := err.(TransientError); !ok {
				obs.IncError()
				log.Errorf("binlog fatal error: %v", err)
				return err
			}
			// transient handling with early returns
			retries++
			obs.IncError()
			log.Warnf("binlog transient error (attempt %d/%d): %v", retries, retryLimit, err)
			if retries > retryLimit {
				log.Errorf("binlog transient error exceeded retries: %v", err)
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			continue
		}

		// success path: reset retry state and process event
		retries = 0
		backoff = initialBackoff
		processReplicationEvent(ev, &currentTxID, sink)
		continue
	}
}

// processReplicationEvent handles replication events and emits RowEvent via sink.
// It mutates currentTxID to track GTID/transaction boundaries.
func processReplicationEvent(ev *replication.BinlogEvent, currentTxID *string, sink func(RowEvent)) {
	switch ev.Header.EventType {
	case replication.GTID_EVENT:
		// TODO: extract GTID string if required; keep empty for now
		*currentTxID = ""
	case replication.QUERY_EVENT:
		qe, _ := ev.Event.(*replication.QueryEvent)
		q := strings.TrimSpace(strings.ToUpper(string(qe.Query)))
		if q == "COMMIT" {
			*currentTxID = ""
		}
		// BEGIN is a marker; GTID should be set by GTID_EVENT previously
	case replication.XID_EVENT:
		*currentTxID = ""
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		sink(RowEvent{TxID: *currentTxID})
	case replication.TABLE_MAP_EVENT:
		// handled elsewhere when mapping to KV
	default:
		// ignore
	}
}

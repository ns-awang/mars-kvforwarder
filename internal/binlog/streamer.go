package binlog

import (
	"context"
	"strings"
	"time"

	obs "mars-kvforwarder/internal/observability"

	"github.com/go-mysql-org/go-mysql/replication"
	aurora "github.com/netSkope/mars-lib/src/database/aurora"
	mlog "github.com/netSkope/mars-lib/src/log"
)

type RowEvent struct {
	TxID      string
	Namespace string
	Pop       string
}

// ExtractNamespacePop parses table name of form `config_data_{namespace}_{POP}`.
func ExtractNamespacePop(tableName string) (namespace, pop string, ok bool) {
	prefix := aurora.CONFIG_DATA_TABLE_PREFIX + "_"
	if !strings.HasPrefix(tableName, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(tableName, prefix)
	idx := strings.LastIndex(rest, "_")
	if idx <= 0 || idx == len(rest)-1 {
		return "", "", false
	}
	namespace = rest[:idx]
	pop = rest[idx+1:]
	if namespace == "" || pop == "" {
		return "", "", false
	}
	return namespace, pop, true
}

// BinlogEventSource is satisfied by *replication.BinlogStreamer; extracted for testability.
type BinlogEventSource interface {
	GetEvent(ctx context.Context) (*replication.BinlogEvent, error)
}

// TransientError marks an error as retryable.
type TransientError struct{ Err error }

func (e TransientError) Error() string { return e.Err.Error() }

// StreamBinlog consumes events from src and invokes sink for each event.
// Retries transient errors up to retryLimit with exponential backoff; stops on fatal errors.
func StreamBinlog(ctx context.Context, src BinlogEventSource, sink func(RowEvent)) (err error) {
	const (
		retryLimit     = 3
		initialBackoff = 100 * time.Millisecond
		maxBackoff     = 3 * time.Second
	)
	log := mlog.GetLogger()
	backoff := initialBackoff
	retries := 0
	state := streamState{
		currentTxID:   "",
		tableIDToName: make(map[uint64]string),
	}
	var ev *replication.BinlogEvent
	for {
		if ctx.Err() != nil {
			return
		}

		ev, err = src.GetEvent(ctx)
		if err != nil {
			if _, ok := err.(TransientError); !ok {
				obs.IncError()
				log.Errorf("binlog fatal error: %v", err)
				return
			}
			retries++
			obs.IncError()
			log.Warnf("binlog transient error (attempt %d/%d): %v", retries, retryLimit, err)
			if retries > retryLimit {
				log.Errorf("binlog transient error exceeded retries: %v", err)
				return
			}
			if ctx.Err() != nil {
				return
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
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

		retries = 0
		backoff = initialBackoff
		processReplicationEvent(ev, &state, sink)
		continue
	}
}

// streamState holds contextual information for processing events.
type streamState struct {
	currentTxID   string
	tableIDToName map[uint64]string
}

// processReplicationEvent handles replication events and emits RowEvent via sink.
func processReplicationEvent(binlogEvent *replication.BinlogEvent, state *streamState, sink func(RowEvent)) {
	switch binlogEvent.Header.EventType {
	case replication.GTID_EVENT:
		state.currentTxID = ""
	case replication.QUERY_EVENT:
		qe, _ := binlogEvent.Event.(*replication.QueryEvent)
		normalizedQuery := strings.TrimSpace(strings.ToUpper(string(qe.Query)))
		if normalizedQuery == "COMMIT" {
			state.currentTxID = ""
		}
	case replication.XID_EVENT:
		state.currentTxID = ""
	case replication.TABLE_MAP_EVENT:
		if tme, ok := binlogEvent.Event.(*replication.TableMapEvent); ok {
			state.tableIDToName[tme.TableID] = string(tme.Table)
		}
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		if rowsEvent, ok := binlogEvent.Event.(*replication.RowsEvent); ok {
			tableName := state.tableIDToName[rowsEvent.TableID]
			namespace, pop, valid := ExtractNamespacePop(tableName)
			if !valid {
				obs.IncError()
				mlog.GetLogger().Warnf("malformed config_data table name, skip row: table=%s", tableName)
				return
			}
			sink(RowEvent{TxID: state.currentTxID, Namespace: namespace, Pop: pop})
		}
	default:
		// ignore
	}
}

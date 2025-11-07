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

// RowEvent represents a single row parsed from the binlog after demarcation.
// Only TxID is modeled here for now; row payload mapping is handled elsewhere.
type RowEvent struct {
	TxID      string
	Namespace string
	Pop       string
}

// Note: prefix comes from mars-lib's CONFIG_DATA_TABLE_PREFIX; we append an underscore for parsing.

// ExtractNamespacePop parses table name of form `${prefix}{namespace}_{POP}` and returns components.
// Returns ok=false if the name does not conform.
func ExtractNamespacePop(tableName, prefix string) (namespace, pop string, ok bool) {
	if !strings.HasPrefix(tableName, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(tableName, prefix)
	// split by last underscore to allow underscores inside namespace in future if needed
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
func StreamBinlog(ctx context.Context, src BinlogEventSource, sink func(RowEvent)) error {
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
		tablePrefix:   aurora.CONFIG_DATA_TABLE_PREFIX + "_",
	}
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
		processReplicationEvent(ev, &state, sink)
		continue
	}
}

// streamState holds contextual information for processing events.
type streamState struct {
	currentTxID   string
	tableIDToName map[uint64]string
	tablePrefix   string
}

// processReplicationEvent handles replication events and emits RowEvent via sink.
func processReplicationEvent(ev *replication.BinlogEvent, st *streamState, sink func(RowEvent)) {
	switch ev.Header.EventType {
	case replication.GTID_EVENT:
		// TODO: extract GTID string if required; keep empty for now
		st.currentTxID = ""
	case replication.QUERY_EVENT:
		qe, _ := ev.Event.(*replication.QueryEvent)
		q := strings.TrimSpace(strings.ToUpper(string(qe.Query)))
		if q == "COMMIT" {
			st.currentTxID = ""
		}
		// BEGIN is a marker; GTID should be set by GTID_EVENT previously
	case replication.XID_EVENT:
		st.currentTxID = ""
	case replication.TABLE_MAP_EVENT:
		if tme, ok := ev.Event.(*replication.TableMapEvent); ok {
			st.tableIDToName[tme.TableID] = string(tme.Table)
		}
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		if re, ok := ev.Event.(*replication.RowsEvent); ok {
			tbl := st.tableIDToName[re.TableID]
			ns, pop, ok2 := ExtractNamespacePop(tbl, st.tablePrefix)
			if !ok2 {
				obs.IncError()
				mlog.GetLogger().Warnf("malformed config_data table name, skip row: table=%s", tbl)
				return
			}
			sink(RowEvent{TxID: st.currentTxID, Namespace: ns, Pop: pop})
		}
	default:
		// ignore
	}
}

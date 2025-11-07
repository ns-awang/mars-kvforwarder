package binlog

import (
	"context"
	"fmt"
	"strings"
	"time"

	obs "mars-kvforwarder/internal/observability"

	agg "mars-kvforwarder/internal/aggregator"

	"github.com/go-mysql-org/go-mysql/replication"
	aurora "github.com/netSkope/mars-lib/src/database/aurora"
	mlog "github.com/netSkope/mars-lib/src/log"
)

var slog = mlog.GetLogger()

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

// StreamBinlog removed; use StreamToChannel

// StreamToChannel streams binlog events and directly emits aggregator.StreamEvent into out.
// This avoids extra adapter layers and lets the Coordinator consume events via channel.
func StreamToChannel(ctx context.Context, src BinlogEventSource, out chan<- agg.StreamEvent) (err error) {
	const (
		retryLimit     = 3
		initialBackoff = 100 * time.Millisecond
		maxBackoff     = 3 * time.Second
	)
	backoff := initialBackoff
	retries := 0
	state := streamState{
		currentTxID:   0,
		tableIDToName: make(map[uint64]string),
	}
	var ev *replication.BinlogEvent
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ev, err = src.GetEvent(ctx)
		if err != nil {
			if _, ok := err.(TransientError); !ok {
				obs.IncError()
				slog.Errorf("binlog fatal error: %v", err)
				return err
			}
			retries++
			obs.IncError()
			slog.Warnf("binlog transient error (attempt %d/%d): %v", retries, retryLimit, err)
			if retries > retryLimit {
				obs.IncError()
				slog.Errorf("binlog transient error exceeded retries: %v", err)
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
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

		switch ev.Header.EventType {
		case replication.GTID_EVENT:
			if ge, ok := ev.Event.(*replication.GTIDEvent); ok {
				state.currentTxID = uint64(ge.GNO)
			} else {
				state.currentTxID = 0
			}
		case replication.QUERY_EVENT:
			qe, _ := ev.Event.(*replication.QueryEvent)
			normalizedQuery := strings.TrimSpace(strings.ToUpper(string(qe.Query)))
			if normalizedQuery == "BEGIN" && state.currentTxID != 0 {
				out <- agg.StreamEvent{Type: agg.TxnBegin, TxID: state.currentTxID}
			}
			if normalizedQuery == "COMMIT" {
				if state.currentTxID != 0 {
					out <- agg.StreamEvent{Type: agg.TxnCommit, TxID: state.currentTxID}
				}
				state.currentTxID = 0
			}
		case replication.XID_EVENT:
			if state.currentTxID != 0 {
				out <- agg.StreamEvent{Type: agg.TxnCommit, TxID: state.currentTxID}
			}
			state.currentTxID = 0
		case replication.TABLE_MAP_EVENT:
			if tme, ok := ev.Event.(*replication.TableMapEvent); ok {
				state.tableIDToName[tme.TableID] = string(tme.Table)
			}
		case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
			replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
			replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
				tableName := state.tableIDToName[rowsEvent.TableID]
				namespace, pop, valid := ExtractNamespacePop(tableName)
				if !valid {
					obs.IncParseError()
					slog.Errorf("malformed config_data table name, skip row: table=%s txid=%d", tableName, state.currentTxID)
					continue
				}
				changes, _, ok2, err2 := handleRowsEvent(ev)
				if err2 != nil {
					obs.IncParseError()
					slog.Errorf("rows mapping error: table=%s txid=%d event=%s err=%v", tableName, state.currentTxID, ev.Header.EventType, err2)
					continue
				}
				if !ok2 {
					continue
				}
				for _, ch := range changes {
					obs.IncProcessedRow()
					out <- agg.StreamEvent{Type: agg.RowChange, TxID: state.currentTxID, Namespace: namespace, Pop: pop, Change: ch}
				}
			}
		default:
			// ignore other events
		}
	}
}

// streamState holds contextual information for processing events.
type streamState struct {
	currentTxID   uint64
	tableIDToName map[uint64]string
}

// processReplicationEvent removed; unified in StreamToChannel

// handleRowsEvent converts a RowsEvent into KV changes and operation.
func handleRowsEvent(e *replication.BinlogEvent) (changes []agg.KVChange, op agg.OperationType, ok bool, err error) {
	rowsEvent, isRows := e.Event.(*replication.RowsEvent)
	if !isRows {
		return nil, "", false, nil
	}
	readAt := time.Now()
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		op = agg.OperationCreate
		changes = make([]agg.KVChange, 0, len(rowsEvent.Rows))
		for _, row := range rowsEvent.Rows {
			ch, valid, mapErr := handleRowChange(row, op)
			if mapErr != nil {
				return nil, "", false, mapErr
			}
			if !valid {
				continue
			}
			ch.ReadAt = readAt
			changes = append(changes, ch)
		}
		return changes, op, true, nil
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		op = agg.OperationUpdate
		if len(rowsEvent.Rows)%2 != 0 {
			return nil, "", false, fmt.Errorf("unexpected UPDATE rows length: %d", len(rowsEvent.Rows))
		}
		changes = make([]agg.KVChange, 0, len(rowsEvent.Rows)/2)
		for i := 0; i < len(rowsEvent.Rows); i += 2 {
			after := rowsEvent.Rows[i+1]
			ch, valid, mapErr := handleRowChange(after, op)
			if mapErr != nil {
				return nil, "", false, mapErr
			}
			if !valid {
				continue
			}
			ch.ReadAt = readAt
			changes = append(changes, ch)
		}
		return changes, op, true, nil
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		op = agg.OperationDelete
		changes = make([]agg.KVChange, 0, len(rowsEvent.Rows))
		for _, row := range rowsEvent.Rows {
			ch, valid, mapErr := handleRowChange(row, op)
			if mapErr != nil {
				return nil, "", false, mapErr
			}
			if !valid {
				continue
			}
			ch.ReadAt = readAt
			changes = append(changes, ch)
		}
		return changes, op, true, nil
	default:
		return nil, "", false, nil
	}
}

// handleRowChange maps a single row to KVChange. Expected columns: [config_key, config_value, tenant]
func handleRowChange(row []interface{}, op agg.OperationType) (ch agg.KVChange, valid bool, err error) {
	if len(row) < 2 {
		return agg.KVChange{}, false, fmt.Errorf("row too short: %d", len(row))
	}
	var keyStr string
	switch v := row[0].(type) {
	case []byte:
		keyStr = string(v)
	case string:
		keyStr = v
	default:
		return agg.KVChange{}, false, fmt.Errorf("unexpected key type: %T", row[0])
	}
	if keyStr == "" {
		return agg.KVChange{}, false, nil
	}
	var valueBytes []byte
	if row[1] == nil {
		valueBytes = nil
	} else {
		switch v := row[1].(type) {
		case []byte:
			valueBytes = v
		case string:
			valueBytes = []byte(v)
		default:
			return agg.KVChange{}, false, fmt.Errorf("unexpected value type: %T", row[1])
		}
	}
	return agg.KVChange{Key: keyStr, Value: valueBytes, Operation: op}, true, nil
}

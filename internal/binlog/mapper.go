package binlog

import (
	"fmt"

	agg "mars-kvforwarder/internal/aggregator"

	"github.com/go-mysql-org/go-mysql/replication"
)

// MapRowsEventToKVChanges converts a RowsEvent into a list of KVChange post-images
// and the associated operation type. Returns ok=false on unsupported event types
// or malformed rows.
func MapRowsEventToKVChanges(e *replication.BinlogEvent) (changes []agg.KVChange, op agg.OperationType, ok bool, err error) {
	rowsEvent, isRows := e.Event.(*replication.RowsEvent)
	if !isRows {
		return nil, "", false, fmt.Errorf("not a RowsEvent")
	}

	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		op = agg.OperationCreate
		for _, row := range rowsEvent.Rows {
			ch, valid, mapErr := mapRowToChange(row, op)
			if mapErr != nil {
				return nil, "", false, mapErr
			}
			if !valid {
				continue
			}
			changes = append(changes, ch)
		}
		return changes, op, true, nil
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		op = agg.OperationUpdate
		// UPDATE produces pairs: [before, after]
		if len(rowsEvent.Rows)%2 != 0 {
			return nil, "", false, fmt.Errorf("unexpected UPDATE rows length: %d", len(rowsEvent.Rows))
		}
		for i := 0; i < len(rowsEvent.Rows); i += 2 {
			after := rowsEvent.Rows[i+1]
			ch, valid, mapErr := mapRowToChange(after, op)
			if mapErr != nil {
				return nil, "", false, mapErr
			}
			if !valid {
				continue
			}
			changes = append(changes, ch)
		}
		return changes, op, true, nil
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		op = agg.OperationDelete
		for _, row := range rowsEvent.Rows {
			ch, valid, mapErr := mapRowToChange(row, op)
			if mapErr != nil {
				return nil, "", false, mapErr
			}
			if !valid {
				continue
			}
			changes = append(changes, ch)
		}
		return changes, op, true, nil
	default:
		return nil, "", false, fmt.Errorf("unsupported rows event type: %v", e.Header.EventType)
	}
}

// mapRowToChange maps a single row (column-ordered values) into KVChange.
// Expected columns: [config_key, config_value, tenant]
func mapRowToChange(row []interface{}, op agg.OperationType) (ch agg.KVChange, valid bool, err error) {
	if len(row) < 2 {
		return agg.KVChange{}, false, fmt.Errorf("row too short: %d", len(row))
	}
	// key at index 0
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
	// value at index 1 (may be nil)
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

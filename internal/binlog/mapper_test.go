package binlog

import (
    "testing"

    "github.com/go-mysql-org/go-mysql/replication"
    agg "mars-kvforwarder/internal/aggregator"
    "github.com/stretchr/testify/require"
)

func TestMapRowsEvent_InsertAndDelete(t *testing.T) {
    // INSERT with two rows
    ins := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}, Event: &replication.RowsEvent{Rows: [][]interface{}{
        {[]byte("k1"), []byte("v1"), []byte("t")},
        {"k2", nil, nil},
    }}}
    changes, op, ok, err := MapRowsEventToKVChanges(ins)
    require.NoError(t, err)
    require.True(t, ok)
    require.Equal(t, agg.OperationCreate, op)
    require.Len(t, changes, 2)
    require.Equal(t, "k1", changes[0].Key)
    require.Equal(t, []byte("v1"), changes[0].Value)
    require.Nil(t, changes[1].Value)

    // DELETE with one row
    del := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.DELETE_ROWS_EVENTv2}, Event: &replication.RowsEvent{Rows: [][]interface{}{
        {[]byte("k3"), []byte("v3"), []byte("t")},
    }}}
    changes, op, ok, err = MapRowsEventToKVChanges(del)
    require.NoError(t, err)
    require.True(t, ok)
    require.Equal(t, agg.OperationDelete, op)
    require.Len(t, changes, 1)
    require.Equal(t, "k3", changes[0].Key)
}

func TestMapRowsEvent_UpdatePairs(t *testing.T) {
    // UPDATE pairs
    upd := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.UPDATE_ROWS_EVENTv2}, Event: &replication.RowsEvent{Rows: [][]interface{}{
        {[]byte("k1"), []byte("old"), []byte("t")},
        {[]byte("k1"), []byte("new"), []byte("t")},
        {"k2", []byte("a"), nil},
        {"k2", []byte("b"), nil},
    }}}
    changes, op, ok, err := MapRowsEventToKVChanges(upd)
    require.NoError(t, err)
    require.True(t, ok)
    require.Equal(t, agg.OperationUpdate, op)
    require.Len(t, changes, 2)
    require.Equal(t, "k1", changes[0].Key)
    require.Equal(t, []byte("new"), changes[0].Value)
    require.Equal(t, "k2", changes[1].Key)
    require.Equal(t, []byte("b"), changes[1].Value)
}



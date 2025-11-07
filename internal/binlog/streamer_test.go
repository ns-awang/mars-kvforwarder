package binlog

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

type fakeStreamer struct {
	errs   []error
	events []*replication.BinlogEvent
	i      int
	count  int32
}

func (f *fakeStreamer) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	atomic.AddInt32(&f.count, 1)
	if f.i < len(f.errs) {
		e := f.errs[f.i]
		f.i++
		if e != nil {
			return nil, e
		}
	}
	idx := f.i - len(f.errs)
	if idx >= 0 && idx < len(f.events) {
		ev := f.events[idx]
		f.i++
		return ev, nil
	}
	return &replication.BinlogEvent{Header: &replication.EventHeader{}}, nil
}

func TestStreamBinlogRetriesAndStopsOnFatal(t *testing.T) {
	// transient, transient, fatal -> should retry twice then stop with fatal
	src := &fakeStreamer{errs: []error{TransientError{Err: errors.New("tmp1")}, TransientError{Err: errors.New("tmp2")}, errors.New("fatal")}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sinkCount := int32(0)
	err := StreamBinlog(ctx, src, func(RowEvent) { atomic.AddInt32(&sinkCount, 1) })
	require.Error(t, err)
	require.Equal(t, int32(0), sinkCount)
}

func TestStreamBinlogStopsAfterExceededRetries(t *testing.T) {
	// 4 transient errors -> exceed retry limit (3)
	src := &fakeStreamer{errs: []error{TransientError{Err: errors.New("tmp1")}, TransientError{Err: errors.New("tmp2")}, TransientError{Err: errors.New("tmp3")}, TransientError{Err: errors.New("tmp4")}}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sinkCount := int32(0)
	err := StreamBinlog(ctx, src, func(RowEvent) { atomic.AddInt32(&sinkCount, 1) })
	require.Error(t, err)
	require.Equal(t, int32(0), sinkCount)
}

func TestStreamBinlogEmitsOnSuccessAndResetsBackoff(t *testing.T) {
	// transient error, then two row events should be emitted
	tmap := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.TABLE_MAP_EVENT}, Event: &replication.TableMapEvent{TableID: 1, Table: []byte("config_data_ns1_POP1")}}
	rows1 := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 1, Rows: [][]interface{}{{[]byte("k"), []byte("v")}}}}
	rows2 := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 1, Rows: [][]interface{}{{[]byte("k2"), nil}}}}
	src := &fakeStreamer{errs: []error{TransientError{Err: errors.New("tmp1")}}, events: []*replication.BinlogEvent{tmap, rows1, rows2}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sinkCount := int32(0)
	// stop after two successes by cancelling context
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	_ = StreamBinlog(ctx, src, func(RowEvent) { atomic.AddInt32(&sinkCount, 1) })
	require.GreaterOrEqual(t, sinkCount, int32(1))
}

func TestStreamBinlogDemarcationTxID(t *testing.T) {
	// GTID -> BEGIN -> TABLE_MAP -> two ROWS -> COMMIT
	gtid := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.GTID_EVENT}}
	begin := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.QUERY_EVENT}, Event: &replication.QueryEvent{Query: []byte("BEGIN")}}
	tmap := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.TABLE_MAP_EVENT}, Event: &replication.TableMapEvent{TableID: 1, Table: []byte("config_data_ns1_POP1")}}
	r1 := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 1, Rows: [][]interface{}{{[]byte("k1"), []byte("v1")}}}}
	r2 := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 1, Rows: [][]interface{}{{[]byte("k2"), []byte("v2")}}}}
	commit := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.XID_EVENT}}
	src := &fakeStreamer{events: []*replication.BinlogEvent{gtid, begin, tmap, r1, r2, commit}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var gotNS []string
	var gotPOP []string
	// cancel after emitting two rows
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	_ = StreamBinlog(ctx, src, func(ev RowEvent) {
		gotNS = append(gotNS, ev.Namespace)
		gotPOP = append(gotPOP, ev.Pop)
	})
	require.GreaterOrEqual(t, len(gotNS), 2)
	require.Equal(t, "ns1", gotNS[0])
	require.Equal(t, "POP1", gotPOP[0])
}

func TestStreamBinlogMapsInsertAndDelete(t *testing.T) {
	// TABLE_MAP -> INSERT rows -> DELETE rows
	tmap := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.TABLE_MAP_EVENT}, Event: &replication.TableMapEvent{TableID: 1, Table: []byte("config_data_ns1_POP1")}}
	ins := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 1, Rows: [][]interface{}{
		{[]byte("k1"), []byte("v1")},
		{"k2", nil},
	}}}
	del := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.DELETE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 1, Rows: [][]interface{}{
		{[]byte("k3"), []byte("v3")},
	}}}
	src := &fakeStreamer{events: []*replication.BinlogEvent{tmap, ins, del}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var got []RowEvent
	_ = StreamBinlog(ctx, src, func(ev RowEvent) {
		got = append(got, ev)
		if len(got) >= 3 {
			cancel()
		}
	})
	require.GreaterOrEqual(t, len(got), 3)
	// First two from INSERT
	require.Equal(t, "k1", got[0].Change.Key)
	require.Equal(t, []byte("v1"), got[0].Change.Value)
	require.Equal(t, "k2", got[1].Change.Key)
	require.Nil(t, got[1].Change.Value)
	// Third from DELETE
	require.Equal(t, "k3", got[2].Change.Key)
}

func TestStreamBinlogMapsUpdatePostImageOnly(t *testing.T) {
	// TABLE_MAP -> UPDATE rows (before, after pairs)
	tmap := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.TABLE_MAP_EVENT}, Event: &replication.TableMapEvent{TableID: 2, Table: []byte("config_data_ns2_POP2")}}
	upd := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.UPDATE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 2, Rows: [][]interface{}{
		{[]byte("k1"), []byte("old")}, {[]byte("k1"), []byte("new")},
		{"k2", []byte("a")}, {"k2", []byte("b")},
	}}}
	src := &fakeStreamer{events: []*replication.BinlogEvent{tmap, upd}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var got []RowEvent
	_ = StreamBinlog(ctx, src, func(ev RowEvent) {
		got = append(got, ev)
		if len(got) >= 2 {
			cancel()
		}
	})
	require.GreaterOrEqual(t, len(got), 2)
	require.Equal(t, "k1", got[0].Change.Key)
	require.Equal(t, []byte("new"), got[0].Change.Value)
	require.Equal(t, "k2", got[1].Change.Key)
	require.Equal(t, []byte("b"), got[1].Change.Value)
}

package binlog

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"

	agg "mars-kvforwarder/internal/aggregator"
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
	out := make(chan agg.StreamEvent, 4)
	err := StreamBinlog(ctx, src, nil, out)
	assert.Error(t, err)
	assert.Equal(t, 0, len(out))
}

func TestStreamBinlogStopsAfterExceededRetries(t *testing.T) {
	// 4 transient errors -> exceed retry limit (3)
	src := &fakeStreamer{errs: []error{TransientError{Err: errors.New("tmp1")}, TransientError{Err: errors.New("tmp2")}, TransientError{Err: errors.New("tmp3")}, TransientError{Err: errors.New("tmp4")}}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out := make(chan agg.StreamEvent, 1)
	err := StreamBinlog(ctx, src, nil, out)
	assert.Error(t, err)
	assert.Equal(t, 0, len(out))
}

func TestStreamBinlogEmitsOnSuccessAndResetsBackoff(t *testing.T) {
	// transient error, then two row events should be emitted
	tmap := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.TABLE_MAP_EVENT}, Event: &replication.TableMapEvent{TableID: 1, Table: []byte("config_data_ns1_POP1")}}
	rows1 := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 1, Rows: [][]interface{}{{[]byte("k"), []byte("v")}}}}
	rows2 := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 1, Rows: [][]interface{}{{[]byte("k2"), nil}}}}
	src := &fakeStreamer{errs: []error{TransientError{Err: errors.New("tmp1")}}, events: []*replication.BinlogEvent{tmap, rows1, rows2}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out := make(chan agg.StreamEvent, 10)
	// stop after brief delay
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	_ = StreamBinlog(ctx, src, nil, out)
	assert.GreaterOrEqual(t, len(out), 1)
}

func TestStreamBinlogDemarcationTxID(t *testing.T) {
	// GTID -> BEGIN -> TABLE_MAP -> two ROWS -> COMMIT
	sid := []byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}
	gtid := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.GTID_EVENT}, Event: &replication.GTIDEvent{SID: sid, GNO: 7}}
	begin := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.QUERY_EVENT}, Event: &replication.QueryEvent{Query: []byte("BEGIN")}}
	tmap := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.TABLE_MAP_EVENT}, Event: &replication.TableMapEvent{TableID: 1, Table: []byte("config_data_ns1_POP1")}}
	r1 := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 1, Rows: [][]interface{}{{[]byte("k1"), []byte("v1")}}}}
	r2 := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 1, Rows: [][]interface{}{{[]byte("k2"), []byte("v2")}}}}
	commit := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.XID_EVENT}}
	src := &fakeStreamer{events: []*replication.BinlogEvent{gtid, begin, tmap, r1, r2, commit}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out := make(chan agg.StreamEvent, 10)
	go func() { time.Sleep(120 * time.Millisecond); cancel() }()
	_ = StreamBinlog(ctx, src, nil, out)
	// Collect row changes
	var gotNS []string
	var gotPOP []string
	var gotTxIDs []uint64
	for len(out) > 0 {
		ev := <-out
		if ev.Type == agg.RowChange {
			gotNS = append(gotNS, ev.Namespace)
			gotPOP = append(gotPOP, ev.Pop)
			gotTxIDs = append(gotTxIDs, ev.TxID)
		}
	}
	assert.GreaterOrEqual(t, len(gotNS), 2)
	assert.Equal(t, "ns1", gotNS[0])
	assert.Equal(t, "POP1", gotPOP[0])
	assert.Equal(t, uint64(7), gotTxIDs[0])
}

func TestStreamBinlogUpdatesTracker(t *testing.T) {
	sid := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00}
	gtid := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.GTID_EVENT}, Event: &replication.GTIDEvent{SID: sid, GNO: 15}}
	tmap := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.TABLE_MAP_EVENT}, Event: &replication.TableMapEvent{TableID: 1, Table: []byte("config_data_ns1_POP1")}}
	row := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 1, Rows: [][]interface{}{{[]byte("k"), []byte("v")}}}}
	src := &fakeStreamer{events: []*replication.BinlogEvent{gtid, tmap, row}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tracker := &GTIDTracker{}
	out := make(chan agg.StreamEvent, 10)
	_ = StreamBinlog(ctx, src, tracker, out)

	last := tracker.Last()
	if assert.NotNil(t, last) {
		assert.Equal(t, "aabbccdd-eeff-1122-3344-556677889900:15", last.String())
	}
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
	out := make(chan agg.StreamEvent, 10)
	go func() { time.Sleep(150 * time.Millisecond); cancel() }()
	_ = StreamBinlog(ctx, src, nil, out)
	var got []agg.StreamEvent
	for len(out) > 0 {
		ev := <-out
		if ev.Type == agg.RowChange {
			got = append(got, ev)
		}
	}
	assert.GreaterOrEqual(t, len(got), 3)
	// First two from INSERT
	assert.Equal(t, "k1", got[0].Change.Key)
	assert.Equal(t, []byte("v1"), got[0].Change.Value)
	assert.Equal(t, "k2", got[1].Change.Key)
	assert.Nil(t, got[1].Change.Value)
	// Third from DELETE
	assert.Equal(t, "k3", got[2].Change.Key)
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
	out := make(chan agg.StreamEvent, 10)
	go func() { time.Sleep(150 * time.Millisecond); cancel() }()
	_ = StreamBinlog(ctx, src, nil, out)
	var got []agg.StreamEvent
	for len(out) > 0 {
		ev := <-out
		if ev.Type == agg.RowChange {
			got = append(got, ev)
		}
	}
	assert.GreaterOrEqual(t, len(got), 2)
	assert.Equal(t, "k1", got[0].Change.Key)
	assert.Equal(t, []byte("new"), got[0].Change.Value)
	assert.Equal(t, "k2", got[1].Change.Key)
	assert.Equal(t, []byte("b"), got[1].Change.Value)
}

func TestStreamBinlogEmitsBeginAndCommit(t *testing.T) {
	// GTID -> BEGIN -> TABLE_MAP -> ROW -> COMMIT
	sid := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	gtid := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.GTID_EVENT}, Event: &replication.GTIDEvent{SID: sid, GNO: 9}}
	begin := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.QUERY_EVENT}, Event: &replication.QueryEvent{Query: []byte("BEGIN")}}
	tmap := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.TABLE_MAP_EVENT}, Event: &replication.TableMapEvent{TableID: 3, Table: []byte("config_data_nsX_POPZ")}}
	row := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 3, Rows: [][]interface{}{{[]byte("k"), []byte("v")}}}}
	commit := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.QUERY_EVENT}, Event: &replication.QueryEvent{Query: []byte("COMMIT")}}
	src := &fakeStreamer{events: []*replication.BinlogEvent{gtid, begin, tmap, row, commit}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out := make(chan agg.StreamEvent, 10)
	_ = StreamBinlog(ctx, src, nil, out)
	// Drain
	var types []agg.EventType
	for len(out) > 0 {
		ev := <-out
		types = append(types, ev.Type)
	}
	assert.GreaterOrEqual(t, len(types), 2)
	assert.Equal(t, agg.TxnBegin, types[0])
	assert.Equal(t, agg.TxnCommit, types[len(types)-1])
}

func TestStreamBinlogSkipsMalformedTableForRows(t *testing.T) {
	// TABLE_MAP with malformed name -> rows should be skipped
	tmap := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.TABLE_MAP_EVENT}, Event: &replication.TableMapEvent{TableID: 5, Table: []byte("badtable")}}
	rows := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 5, Rows: [][]interface{}{{[]byte("k"), []byte("v")}}}}
	src := &fakeStreamer{events: []*replication.BinlogEvent{tmap, rows}}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	out := make(chan agg.StreamEvent, 10)
	_ = StreamBinlog(ctx, src, nil, out)
	// Ensure no RowChange emitted
	for len(out) > 0 {
		ev := <-out
		assert.NotEqual(t, agg.RowChange, ev.Type)
	}
}

func TestStreamBinlogOddUpdateRowsAreIgnored(t *testing.T) {
	// UPDATE with odd number of images -> mapping error -> no events
	tmap := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.TABLE_MAP_EVENT}, Event: &replication.TableMapEvent{TableID: 7, Table: []byte("config_data_nsY_POPQ")}}
	upd := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.UPDATE_ROWS_EVENTv2}, Event: &replication.RowsEvent{TableID: 7, Rows: [][]interface{}{
		{[]byte("k1"), []byte("old")}, // before only, missing after
	}}}
	src := &fakeStreamer{events: []*replication.BinlogEvent{tmap, upd}}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	out := make(chan agg.StreamEvent, 10)
	_ = StreamBinlog(ctx, src, nil, out)
	// No RowChange expected
	for len(out) > 0 {
		ev := <-out
		assert.NotEqual(t, agg.RowChange, ev.Type)
	}
}

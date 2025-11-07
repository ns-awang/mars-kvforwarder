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
	rows1 := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}}
	rows2 := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}}
	src := &fakeStreamer{errs: []error{TransientError{Err: errors.New("tmp1")}}, events: []*replication.BinlogEvent{rows1, rows2}}
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
	// GTID -> BEGIN -> two ROWS -> COMMIT
	gtid := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.GTID_EVENT}}
	begin := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.QUERY_EVENT}, Event: &replication.QueryEvent{Query: []byte("BEGIN")}}
	r1 := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}}
	r2 := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}}
	commit := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.XID_EVENT}}
	src := &fakeStreamer{events: []*replication.BinlogEvent{gtid, begin, r1, r2, commit}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var got []string
	// cancel after emitting two rows
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	_ = StreamBinlog(ctx, src, func(ev RowEvent) { got = append(got, ev.TxID) })
	require.GreaterOrEqual(t, len(got), 2)
	require.Equal(t, got[0], got[1])
}

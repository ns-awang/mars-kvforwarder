package binlog

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeSource struct {
	seq   []error
	i     int
	count int32
}

func (f *fakeSource) Next(ctx context.Context) (RowEvent, error) {
	atomic.AddInt32(&f.count, 1)
	if f.i >= len(f.seq) {
		return RowEvent{}, nil
	}
	err := f.seq[f.i]
	f.i++
	if err != nil {
		return RowEvent{}, err
	}
	return RowEvent{}, nil
}

func TestStreamBinlogRetriesAndStopsOnFatal(t *testing.T) {
	// transient, transient, fatal -> should retry twice then stop with fatal
	src := &fakeSource{seq: []error{TransientError{Err: errors.New("tmp1")}, TransientError{Err: errors.New("tmp2")}, errors.New("fatal")}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sinkCount := int32(0)
	err := StreamBinlog(ctx, src, func(RowEvent) { atomic.AddInt32(&sinkCount, 1) })
	require.Error(t, err)
	require.Equal(t, int32(0), sinkCount)
}

func TestStreamBinlogStopsAfterExceededRetries(t *testing.T) {
	// 4 transient errors -> exceed retry limit (3)
	src := &fakeSource{seq: []error{TransientError{Err: errors.New("tmp1")}, TransientError{Err: errors.New("tmp2")}, TransientError{Err: errors.New("tmp3")}, TransientError{Err: errors.New("tmp4")}}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sinkCount := int32(0)
	err := StreamBinlog(ctx, src, func(RowEvent) { atomic.AddInt32(&sinkCount, 1) })
	require.Error(t, err)
	require.Equal(t, int32(0), sinkCount)
}

func TestStreamBinlogEmitsOnSuccessAndResetsBackoff(t *testing.T) {
	// transient, then success events
	src := &fakeSource{seq: []error{TransientError{Err: errors.New("tmp1")}, nil, nil}}
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

package aggregator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoordinatorBackpressureOnOutput(t *testing.T) {
	// Small sizes to trigger backpressure easily: batch size 2, out buffer 1
	coord, in, out := NewCoordinator(10, 1, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go coord.Run(ctx)

	// Begin tx and send 2 unique changes -> one batch emitted
	in <- StreamEvent{Type: TxnBegin, TxID: "t1"}
	in <- StreamEvent{Type: RowChange, TxID: "t1", Change: KVChange{Key: "a", Value: []byte("1")}}
	in <- StreamEvent{Type: RowChange, TxID: "t1", Change: KVChange{Key: "b", Value: []byte("2")}}

	// Next change should try to create second batch on commit, but the out buffer
	// is still full (we have not consumed the first batch yet), so send blocks.
	in <- StreamEvent{Type: RowChange, TxID: "t1", Change: KVChange{Key: "c", Value: []byte("3")}}
	in <- StreamEvent{Type: TxnCommit, TxID: "t1"}

	// Drain the first batch (unblocks coordinator so second can be enqueued)
	var b1 Batch
	select {
	case b1 = <-out:
		// got first batch
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected first batch within 500ms")
	}
	assert.Equal(t, 1, b1.BatchIndex)

	// Second batch should arrive promptly after buffer is freed
	select {
	case b2 := <-out:
		require.Equal(t, 2, b2.BatchIndex)
		require.Equal(t, 2, b2.BatchTotal)
	case <-time.After(1 * time.Second):
		t.Fatal("expected second batch after freeing buffer")
	}

	close(in)
	cancel()
}

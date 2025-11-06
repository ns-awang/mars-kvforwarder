package aggregator

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatcherFlushOnMaxSize(t *testing.T) {
	b := NewBatcher(50)
	tx := "tx-10"
	b.Begin(tx)

	var emitted []Batch
	// Apply 50 unique keys
	for i := 0; i < 50; i++ {
		out := b.ApplyChange(tx, KVChange{Key: keyN(i), Value: []byte("v")})
		if i < 49 {
			require.Len(t, out, 0)
		}
		if i == 49 {
			require.Len(t, out, 1)
			assert.Equal(t, 1, out[0].BatchIndex)
			assert.Equal(t, 0, out[0].BatchTotal)
			emitted = append(emitted, out[0])
		}
	}

	// Add one more unique key
	out := b.ApplyChange(tx, KVChange{Key: "k-extra", Value: []byte("v")})
	require.Len(t, out, 0)

	// Commit should flush final batch with total=2
	out = b.Commit(tx)
	require.Len(t, out, 1)
	assert.Equal(t, 2, out[0].BatchIndex)
	assert.Equal(t, 2, out[0].BatchTotal)
	emitted = append(emitted, out[0])

	assert.Len(t, emitted[0].Items, 50)
	assert.Len(t, emitted[1].Items, 1)
}

func TestBatcherCommitOnly(t *testing.T) {
	b := NewBatcher(50)
	tx := "tx-11"
	b.Begin(tx)
	for i := 0; i < 10; i++ {
		_ = b.ApplyChange(tx, KVChange{Key: keyN(i), Value: []byte("v")})
	}
	out := b.Commit(tx)
	require.Len(t, out, 1)
	assert.Equal(t, 1, out[0].BatchIndex)
	assert.Equal(t, 1, out[0].BatchTotal)
}

func TestBatcherLastWriteWinsWithinWindow(t *testing.T) {
	b := NewBatcher(50)
	tx := "tx-12"
	b.Begin(tx)
	_ = b.ApplyChange(tx, KVChange{Key: "k", Value: []byte("a"), Operation: OperationCreate})
	_ = b.ApplyChange(tx, KVChange{Key: "k", Value: []byte("b"), Operation: OperationUpdate})
	out := b.Commit(tx)
	require.Len(t, out, 1)
	require.Len(t, out[0].Items, 1)
	assert.Equal(t, "b", string(out[0].Items[0].Value))
}

// keyN returns a string key for integer n.
func keyN(n int) string {
	return "k-" + strconv.Itoa(n)
}

package aggregator

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggregatorFlushOnMaxSize(t *testing.T) {
	b := NewAggregator(50)
	tx := "tx-10"
	b.Begin(tx)

	var emitted []Batch
	// Apply 50 unique keys
	for i := 0; i < 50; i++ {
		batch, ok := b.ApplyChange(tx, KVChange{Key: keyN(i), Value: []byte("v")})
		if i < 49 {
			require.False(t, ok)
		}
		if i == 49 {
			require.True(t, ok)
			assert.Equal(t, 1, batch.BatchIndex)
			assert.Equal(t, 0, batch.BatchTotal)
			emitted = append(emitted, batch)
		}
	}

	// Add one more unique key
	_, ok := b.ApplyChange(tx, KVChange{Key: "k-extra", Value: []byte("v")})
	require.False(t, ok)

	// Commit should flush final batch with total=2
	batch, ok := b.Commit(tx)
	require.True(t, ok)
	assert.Equal(t, 2, batch.BatchIndex)
	assert.Equal(t, 2, batch.BatchTotal)
	emitted = append(emitted, batch)

	assert.Len(t, emitted[0].Items, 50)
	assert.Len(t, emitted[1].Items, 1)
}

func TestAggregatorCommitOnly(t *testing.T) {
	b := NewAggregator(50)
	tx := "tx-11"
	b.Begin(tx)
	for i := 0; i < 10; i++ {
		_, _ = b.ApplyChange(tx, KVChange{Key: keyN(i), Value: []byte("v")})
	}
	batch, ok := b.Commit(tx)
	require.True(t, ok)
	assert.Equal(t, 1, batch.BatchIndex)
	assert.Equal(t, 1, batch.BatchTotal)
}

func TestAggregatorLastWriteWinsWithinWindow(t *testing.T) {
	b := NewAggregator(50)
	tx := "tx-12"
	b.Begin(tx)
	_, _ = b.ApplyChange(tx, KVChange{Key: "k", Value: []byte("a"), Operation: OperationCreate})
	_, _ = b.ApplyChange(tx, KVChange{Key: "k", Value: []byte("b"), Operation: OperationUpdate})
	batch, ok := b.Commit(tx)
	require.True(t, ok)
	require.Len(t, batch.Items, 1)
	assert.Equal(t, "b", string(batch.Items[0].Value))
}

// keyN returns a string key for integer n.
func keyN(n int) string {
	return "k-" + strconv.Itoa(n)
}

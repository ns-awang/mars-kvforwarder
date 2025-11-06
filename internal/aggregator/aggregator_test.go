package aggregator

import (
	"crypto/sha256"
	"encoding/hex"
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

func TestHeadersAcrossSplitBatches(t *testing.T) {
	a := NewAggregator(2) // force split after 2 unique keys
	tx := "tx-split"
	a.Begin(tx)

	// First two changes -> first mid-tx batch (index=1, total=0)
	if batch, ok := a.ApplyChange(tx, KVChange{Key: "k1", Value: []byte("v1"), Operation: OperationCreate}); ok {
		t.Fatalf("unexpected batch after first change: %+v", batch)
	}
	batch1, ok := a.ApplyChange(tx, KVChange{Key: "k2", Value: []byte("v2"), Operation: OperationUpdate})
	require.True(t, ok)
	assert.Equal(t, 1, batch1.BatchIndex)
	assert.Equal(t, 0, batch1.BatchTotal) // mid-tx

	// Verify checksum is key+value+op
	h := sha256.New()
	for _, it := range batch1.Items {
		h.Write([]byte(it.Key))
		h.Write(it.Value)
		h.Write([]byte(it.Operation))
	}
	assert.Equal(t, hex.EncodeToString(h.Sum(nil)), batch1.PayloadSHA256)

	// Add another change so commit has remaining items to flush
	if _, ok := a.ApplyChange(tx, KVChange{Key: "k3", Value: []byte("v3"), Operation: OperationDelete}); ok {
		t.Fatalf("did not expect a second batch before commit")
	}

	// Commit -> final batch (index=2, total=2)
	batch2, ok := a.Commit(tx)
	require.True(t, ok)
	assert.Equal(t, 2, batch2.BatchIndex)
	assert.Equal(t, 2, batch2.BatchTotal)

	// Headers for both batches
	h1 := BuildHeaders(batch1, "prev1", "curr1")
	h2 := BuildHeaders(batch2, "prev2", "curr2")
	// Basic spot-checks
	m1 := map[string]string{}
	for _, hd := range h1 {
		m1[hd.Key] = string(hd.Value)
	}
	m2 := map[string]string{}
	for _, hd := range h2 {
		m2[hd.Key] = string(hd.Value)
	}

	assert.Equal(t, "1", m1[HeaderBatchIndex])
	assert.Equal(t, "0", m1[HeaderBatchTotal])
	assert.Equal(t, "2", m2[HeaderBatchIndex])
	assert.Equal(t, "2", m2[HeaderBatchTotal])
}

// keyN returns a string key for integer n.
func keyN(n int) string {
	return "k-" + strconv.Itoa(n)
}

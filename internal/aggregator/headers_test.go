package aggregator

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildHeadersIncludesRequiredFields(t *testing.T) {
	// Build a real batch via aggregator to verify checksum correctness
	b := NewAggregator(50)
	tx := "tx-100"
	b.Begin(tx)
    _, _ = b.ApplyChange(tx, KVChange{Key: "a", Value: []byte("foo")})
    _, _ = b.ApplyChange(tx, KVChange{Key: "b", Value: []byte("bar")})
    batch, ok := b.Commit(tx)
    require.True(t, ok)

	// Recompute expected sha256 of key+value+operation per item
	h := sha256.New()
	for _, it := range batch.Items {
		h.Write([]byte(it.Key))
		h.Write(it.Value)
		h.Write([]byte(it.Operation))
	}
	expectedSHA := hex.EncodeToString(h.Sum(nil))
	assert.Equal(t, expectedSHA, batch.PayloadSHA256)

	prev := "prev-gtid"
	curr := "curr-gtid"
	headers := BuildHeaders(batch, prev, curr)

	// Collect headers into a map for easy lookup
	m := map[string]string{}
	for _, hd := range headers {
		m[hd.Key] = string(hd.Value)
	}

	assert.Equal(t, expectedSHA, m[HeaderSHA256])
	assert.Equal(t, prev, m[HeaderPrevTxID])
	assert.Equal(t, curr, m[HeaderCurrTxID])
	assert.Equal(t, "1", m[HeaderBatchIndex])
	assert.Equal(t, "1", m[HeaderBatchTotal])
}

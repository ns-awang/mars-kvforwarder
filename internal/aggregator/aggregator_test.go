package aggregator

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLastWriteWinsWithinTransaction(t *testing.T) {
	a := New()
	tx := "tx-1"
	require.NoError(t, a.Begin(tx))

	// Same key updated multiple times; last write should win
	require.NoError(t, a.ApplyChange(tx, KVChange{Key: "k1", Value: []byte("1"), Operation: OperationCreate}))
	require.NoError(t, a.ApplyChange(tx, KVChange{Key: "k1", Value: []byte("2"), Operation: OperationUpdate}))
	require.NoError(t, a.ApplyChange(tx, KVChange{Key: "k1", Value: []byte("3"), Operation: OperationUpdate}))

	agg, err := a.Commit(tx)
	require.NoError(t, err)
	require.Len(t, agg.Items, 1)
	assert.True(t, bytes.Equal(agg.Items[0].Value, []byte("3")))
}

func TestMultipleKeysDeterministicOrder(t *testing.T) {
	a := New()
	tx := "tx-2"
	require.NoError(t, a.Begin(tx))

	require.NoError(t, a.ApplyChange(tx, KVChange{Key: "a", Value: []byte("x=1"), Operation: OperationCreate}))
	require.NoError(t, a.ApplyChange(tx, KVChange{Key: "b", Value: []byte("y=2"), Operation: OperationUpdate}))
	// Update key a again; should remain first in ordering
	require.NoError(t, a.ApplyChange(tx, KVChange{Key: "a", Value: []byte("x=9"), Operation: OperationUpdate}))

	agg, err := a.Commit(tx)
	require.NoError(t, err)
	require.Len(t, agg.Items, 2)
	assert.Equal(t, "a", agg.Items[0].Key)
	assert.Equal(t, "b", agg.Items[1].Key)
	assert.True(t, bytes.Equal(agg.Items[0].Value, []byte("x=9")))
}

func TestBeginTwiceError(t *testing.T) {
	a := New()
	tx := "tx-3"
	require.NoError(t, a.Begin(tx))
	assert.Error(t, a.Begin(tx))
}

func TestApplyWithoutBeginError(t *testing.T) {
	a := New()
	tx := "tx-4"
	assert.Error(t, a.ApplyChange(tx, KVChange{Key: "k", Value: []byte("1"), Operation: OperationCreate}))
}

func TestAbortRemovesBuffer(t *testing.T) {
	a := New()
	tx := "tx-5"
	require.NoError(t, a.Begin(tx))
	assert.Equal(t, 1, a.PendingTxCount())
	require.NoError(t, a.Abort(tx))
	assert.Equal(t, 0, a.PendingTxCount())
}

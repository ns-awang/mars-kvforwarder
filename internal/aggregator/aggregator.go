package aggregator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	obs "mars-kvforwarder/internal/observability"

	mlog "github.com/netSkope/mars-lib/src/log"
)

// cache logger to avoid repeated lookups
var slog = mlog.GetLogger()

// Batch represents an emitted batch of KV changes for a transaction.
// BatchTotal is 0 for mid-transaction batches (unknown until commit). The final
// commit batch will set BatchTotal to the final total across the transaction.
// IdempotencyKey should be deterministic (e.g., txID:batchIndex) and can be
// combined with headers downstream.
type Batch struct {
	TxID           uint64
	BatchIndex     int
	BatchTotal     int
	Items          []KVChange
	IdempotencyKey string
	PayloadSHA256  string
}

// Aggregator handles batching KV changes per transaction with last-write-wins within
// each batch window, flushing on max size or commit.
type Aggregator struct {
	MaxBatchSize int
	txs          map[uint64]*txBatchState
}

type txBatchState struct {
	pendingKV map[string]KVChange
	index     int
}

// DefaultMaxBatchSize is used if a non-positive max batch size is provided.
const DefaultMaxBatchSize = 50

// NewAggregator creates a new Aggregator with DefaultMaxBatchSize.
func NewAggregator() *Aggregator {
	obs.Init(nil)
	return &Aggregator{
		MaxBatchSize: DefaultMaxBatchSize,
		txs:          make(map[uint64]*txBatchState),
	}
}

func (b *Aggregator) Begin(txID uint64) {
	if _, ok := b.txs[txID]; !ok {
		b.txs[txID] = &txBatchState{
			pendingKV: make(map[string]KVChange),
			index:     0,
		}
	}
}

// ApplyChange adds a change to the current batch window. If the number of unique
// keys in the window reaches MaxBatchSize, a batch is emitted with BatchTotal=0.
// ApplyChange applies a change; returns a batch if threshold reached.
func (b *Aggregator) ApplyChange(txID uint64, change KVChange) (Batch, bool) {
	if change.Key == "" {
		slog.Warnf("skip change with empty key: tx=%d", txID)
		return Batch{}, false
	}

	state, ok := b.txs[txID]
	if !ok {
		state = &txBatchState{pendingKV: make(map[string]KVChange)}
		b.txs[txID] = state
	}

	state.pendingKV[change.Key] = change

	if len(state.pendingKV) >= b.MaxBatchSize {
		return b.flushBatch(txID, state), true
	}
	return Batch{}, false
}

// Commit finalizes the transaction, emitting any remaining items as the final batch.
// The final batch's BatchTotal will be set to the final count of batches for this tx.
func (b *Aggregator) Commit(txID uint64) (Batch, bool) {

	state, ok := b.txs[txID]
	if !ok {
		slog.Errorf("commit on unknown tx: tx=%d", txID)
		return Batch{}, false
	}

	// If there are remaining items, emit a final batch.
	if len(state.pendingKV) > 0 {
		batch := b.flushBatch(txID, state)
		// Now that we know total (state.index), set BatchTotal on the final batch.
		batch.BatchTotal = state.index
		delete(b.txs, txID)
		return batch, true
	}

	delete(b.txs, txID)
	return Batch{}, false
}

// flushBatch materializes the current window into a Batch and resets the window.
func (b *Aggregator) flushBatch(txID uint64, state *txBatchState) Batch {
	items := make([]KVChange, 0, len(state.pendingKV))
	for _, ch := range state.pendingKV {
		items = append(items, ch)
	}
	state.index++
	batch := Batch{
		TxID:           txID,
		BatchIndex:     state.index,
		BatchTotal:     0,
		Items:          items,
		IdempotencyKey: fmt.Sprintf("%d:%d", txID, state.index),
	}
	// Compute SHA256 across key, value, and operation for each item
	h := sha256.New()
	for _, it := range items {
		h.Write([]byte(it.Key))
		h.Write(it.Value)
		h.Write([]byte(it.Operation))
	}
	batch.PayloadSHA256 = hex.EncodeToString(h.Sum(nil))

	// Metrics + Log flush event (latency from tx begin)
	var secs float64
	// Compute latency from earliest read timestamp among items in this batch.
	var earliest time.Time
	for _, it := range items {
		if it.ReadAt.IsZero() {
			continue
		}
		if earliest.IsZero() || it.ReadAt.Before(earliest) {
			earliest = it.ReadAt
		}
	}
	if !earliest.IsZero() {
		secs = time.Since(earliest).Seconds()
	}
	obs.ObserveFlush(len(items), secs)
	slog.Infof("batch flush: tx=%d index=%d items=%d", batch.TxID, batch.BatchIndex, len(batch.Items))

	// reset window
	state.pendingKV = make(map[string]KVChange)

	return batch
}

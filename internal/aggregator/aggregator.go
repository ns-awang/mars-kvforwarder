package aggregator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	obs "mars-kvforwarder/internal/observability"

	mlog "github.com/netSkope/mars-lib/src/log"
)

// Batch represents an emitted batch of KV changes for a transaction.
// BatchTotal is 0 for mid-transaction batches (unknown until commit). The final
// commit batch will set BatchTotal to the final total across the transaction.
// IdempotencyKey should be deterministic (e.g., txID:batchIndex) and can be
// combined with headers downstream.
type Batch struct {
	TxID           string
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
	txs          map[string]*txBatchState
}

type txBatchState struct {
	current map[string]KVChange
	order   []string
	index   int
}

// DefaultMaxBatchSize is used if a non-positive max batch size is provided.
const DefaultMaxBatchSize = 50

// NewAggregator creates a new Aggregator with the provided max batch size. If
// maxBatchSize <= 0, a safe default is used.
func NewAggregator(maxBatchSize int) *Aggregator {
	obs.Init(nil)
	if maxBatchSize <= 0 {
		maxBatchSize = DefaultMaxBatchSize
	}
	return &Aggregator{
		MaxBatchSize: maxBatchSize,
		txs:          make(map[string]*txBatchState),
	}
}

func (b *Aggregator) Begin(txID string) {
	if _, ok := b.txs[txID]; !ok {
		b.txs[txID] = &txBatchState{
			current: make(map[string]KVChange),
			order:   make([]string, 0, b.MaxBatchSize),
			index:   0,
		}
	}
}

// ApplyChange adds a change to the current batch window. If the number of unique
// keys in the window reaches MaxBatchSize, a batch is emitted with BatchTotal=0.
// ApplyChange applies a change; returns a batch if threshold reached.
func (b *Aggregator) ApplyChange(txID string, change KVChange) (Batch, bool) {

	state, ok := b.txs[txID]
	if !ok {
		state = &txBatchState{current: make(map[string]KVChange), order: make([]string, 0, b.MaxBatchSize)}
		b.txs[txID] = state
	}

	if _, exists := state.current[change.Key]; !exists {
		state.order = append(state.order, change.Key)
	}
	state.current[change.Key] = change

	if len(state.current) >= b.MaxBatchSize {
		return b.emitLocked(txID, state), true
	}
	return Batch{}, false
}

// Commit finalizes the transaction, emitting any remaining items as the final batch.
// The final batch's BatchTotal will be set to the final count of batches for this tx.
// Commit finalizes the transaction, emitting any remaining items as the final batch.
func (b *Aggregator) Commit(txID string) (Batch, bool) {

	state, ok := b.txs[txID]
	if !ok {
		return Batch{}, false
	}

	// If there are remaining items, emit a final batch.
	if len(state.current) > 0 {
		batch := b.emitLocked(txID, state)
		// Now that we know total (state.index), set BatchTotal on the final batch.
		batch.BatchTotal = state.index
		delete(b.txs, txID)
		return batch, true
	}

	delete(b.txs, txID)
	return Batch{}, false
}

// emitLocked emits the current batch window and resets it. If final is true, BatchTotal
// will be set by the caller after the total is known.
func (b *Aggregator) emitLocked(txID string, state *txBatchState) Batch {
	items := make([]KVChange, 0, len(state.order))
	for _, k := range state.order {
		if ch, ok := state.current[k]; ok {
			items = append(items, ch)
		}
	}
	state.index++
	batch := Batch{
		TxID:           txID,
		BatchIndex:     state.index,
		BatchTotal:     0,
		Items:          items,
		IdempotencyKey: fmt.Sprintf("%s:%d", txID, state.index),
	}
	// Compute SHA256 across key, value, and operation for each item
	h := sha256.New()
	for _, it := range items {
		h.Write([]byte(it.Key))
		h.Write(it.Value)
		h.Write([]byte(it.Operation))
	}
	batch.PayloadSHA256 = hex.EncodeToString(h.Sum(nil))

	// Metrics + Log flush event
	obs.ObserveFlush(len(items), 0)
	mlog.GetLogger().Infof("batch flush: tx=%s index=%d total=%d items=%d sha256=%s", batch.TxID, batch.BatchIndex, batch.BatchTotal, len(batch.Items), batch.PayloadSHA256)

	// reset window
	state.current = make(map[string]KVChange)
	state.order = state.order[:0]

	return batch
}

package aggregator

import (
	"errors"
	"sync"
)

// OperationType represents the kind of change for a KV pair.
type OperationType string

const (
	OperationCreate OperationType = "create"
	OperationUpdate OperationType = "update"
	OperationDelete OperationType = "delete"
)

// KVChange captures a single key-value change with its operation type.
type KVChange struct {
	Key string
	// Value contains the serialized payload for this key. Semantics are last-write-wins per tx.
	Value     []byte
	Operation OperationType
}

// AggregatedTransaction is the output for a committed transaction after aggregation.
type AggregatedTransaction struct {
	TxID  string
	Items []KVChange
}

// txBuffer stores in-flight aggregation state for a single transaction.
type txBuffer struct {
	txID string
	// lastWriteWins holds the final KVChange per key (last change in this tx wins)
	lastWriteWins map[string]KVChange
	// keyOrder preserves first-seen order of keys for deterministic output ordering
	keyOrder []string
	began    bool
}

// Aggregator maintains per-transaction buffers and provides last-write-wins aggregation.
type Aggregator struct {
	mu        sync.Mutex
	txBuffers map[string]*txBuffer
}

// New creates a new Aggregator instance.
func New() *Aggregator {
	return &Aggregator{txBuffers: make(map[string]*txBuffer)}
}

var (
	ErrTxAlreadyBegan = errors.New("transaction already began")
	ErrTxNotStarted   = errors.New("transaction not started")
	ErrUnknownTx      = errors.New("unknown transaction")
)

// Begin initializes a buffer for a transaction. Returns error if called twice for same tx.
func (a *Aggregator) Begin(txID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := a.txBuffers[txID]; ok {
		return ErrTxAlreadyBegan
	}
	a.txBuffers[txID] = &txBuffer{
		txID:          txID,
		lastWriteWins: make(map[string]KVChange),
		keyOrder:      make([]string, 0, 16),
		began:         true,
	}
	return nil
}

// ApplyChange applies a KV change to the given transaction using last-write-wins semantics.
func (a *Aggregator) ApplyChange(txID string, change KVChange) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	buf, ok := a.txBuffers[txID]
	if !ok || !buf.began {
		return ErrTxNotStarted
	}
	if _, exists := buf.lastWriteWins[change.Key]; !exists {
		buf.keyOrder = append(buf.keyOrder, change.Key)
	}
	buf.lastWriteWins[change.Key] = change
	return nil
}

// Commit finalizes aggregation for the transaction and removes its buffer.
func (a *Aggregator) Commit(txID string) (AggregatedTransaction, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	buf, ok := a.txBuffers[txID]
	if !ok {
		return AggregatedTransaction{}, ErrUnknownTx
	}

	// Build deterministic ordered result from keyOrder with last-write-wins values
	items := make([]KVChange, 0, len(buf.keyOrder))
	for _, k := range buf.keyOrder {
		if ch, exists := buf.lastWriteWins[k]; exists {
			items = append(items, ch)
		}
	}

	delete(a.txBuffers, txID)
	return AggregatedTransaction{TxID: txID, Items: items}, nil
}

// Abort discards the aggregation state for the given transaction.
func (a *Aggregator) Abort(txID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.txBuffers[txID]; !ok {
		return ErrUnknownTx
	}
	delete(a.txBuffers, txID)
	return nil
}

// PendingTxCount returns the number of in-flight transactions.
func (a *Aggregator) PendingTxCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.txBuffers)
}

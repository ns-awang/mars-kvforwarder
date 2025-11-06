package aggregator

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
	// Value contains the serialized payload for this key. Semantics are last-write-wins.
	Value     []byte
	Operation OperationType
}

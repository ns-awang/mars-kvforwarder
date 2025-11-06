package aggregator

// EventKind identifies the type of incoming stream event.
type EventType int

const (
	TxnBegin EventType = iota
	RowChange
	TxnCommit
)

// StreamEvent represents an input event from the binlog streaming component.
type StreamEvent struct {
	Type   EventType
	TxID   string
	Change KVChange // only for EventChange
}

// Coordinator wires the aggregator to input/output channels and enforces backpressure
// via a bounded output channel. It does not handle GTID persistence.
type Coordinator struct {
	in         chan StreamEvent
	out        chan Batch
	aggregator *Aggregator
}

// NewCoordinator creates a coordinator with bounded input/output channels.
func NewCoordinator(inputBufferSize, outputBufferSize, maxBatchSize int) (*Coordinator, chan<- StreamEvent, <-chan Batch) {
	c := &Coordinator{
		in:         make(chan StreamEvent, inputBufferSize),
		out:        make(chan Batch, outputBufferSize),
		aggregator: NewAggregator(maxBatchSize),
	}
	return c, c.in, c.out
}

// Run processes events until the input channel is closed.
func (c *Coordinator) Run() {
	for ev := range c.in {
		switch ev.Type {
		case TxnBegin:
			c.aggregator.Begin(ev.TxID)
		case RowChange:
			if batch, ok := c.aggregator.ApplyChange(ev.TxID, ev.Change); ok {
				c.out <- batch // blocks when output buffer is full (backpressure)
			}
		case TxnCommit:
			if batch, ok := c.aggregator.Commit(ev.TxID); ok {
				c.out <- batch // blocks until consumer catches up
			}
		}
	}
}

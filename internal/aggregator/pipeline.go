package aggregator

import "context"

// EventType identifies the type of incoming stream event.
type EventType int

const (
	TxnBegin EventType = iota
	RowChange
	TxnCommit
)

// StreamEvent represents an input event from the binlog streaming component.
type StreamEvent struct {
	Type      EventType
	TxID      uint64
	Namespace string   // only set for RowChange
	Pop       string   // only set for RowChange
	Change    KVChange // only for RowChange
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
		aggregator: NewAggregator(),
	}
	// honor caller-provided batch size (tests rely on this); ignore if <=0
	if maxBatchSize > 0 {
		c.aggregator.MaxBatchSize = maxBatchSize
	}
	return c, c.in, c.out
}

// Run processes events until the input channel is closed.
func (c *Coordinator) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-c.in:
			if !ok {
				return
			}
			switch ev.Type {
			case TxnBegin:
				c.aggregator.Begin(ev.TxID)
			case RowChange:
				if batch, hasBatch := c.aggregator.ApplyChange(ev.TxID, ev.Change); hasBatch {
					c.out <- batch // blocks when output buffer is full (backpressure)
				}
			case TxnCommit:
				if batch, hasBatch := c.aggregator.Commit(ev.TxID); hasBatch {
					c.out <- batch // blocks until consumer catches up
				}
			}
		}
	}
}

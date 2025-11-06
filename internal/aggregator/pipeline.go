package aggregator

// EventKind identifies the type of incoming stream event.
type EventKind int

const (
    EventBegin EventKind = iota
    EventChange
    EventCommit
)

// StreamEvent represents an input event from the binlog streaming component.
type StreamEvent struct {
    Kind   EventKind
    TxID   string
    Change KVChange // only for EventChange
}

// Coordinator wires the batcher to input/output channels and enforces backpressure
// via a bounded output channel. It does not handle GTID persistence.
type Coordinator struct {
    in       chan StreamEvent
    out      chan Batch
    batcher  *Aggregator
}

// NewCoordinator creates a coordinator with bounded input/output channels.
func NewCoordinator(inputBufferSize, outputBufferSize, maxBatchSize int) (*Coordinator, chan<- StreamEvent, <-chan Batch) {
    c := &Coordinator{
        in:      make(chan StreamEvent, inputBufferSize),
        out:     make(chan Batch, outputBufferSize),
        batcher: NewAggregator(maxBatchSize),
    }
    return c, c.in, c.out
}

// Run processes events until the input channel is closed.
func (c *Coordinator) Run() {
    for ev := range c.in {
        switch ev.Kind {
        case EventBegin:
            c.batcher.Begin(ev.TxID)
        case EventChange:
            if batches := c.batcher.ApplyChange(ev.TxID, ev.Change); len(batches) > 0 {
                for _, b := range batches {
                    c.out <- b // blocks when output buffer is full (backpressure)
                }
            }
        case EventCommit:
            if batches := c.batcher.Commit(ev.TxID); len(batches) > 0 {
                for _, b := range batches {
                    c.out <- b // blocks until consumer catches up
                }
            }
        }
    }
}



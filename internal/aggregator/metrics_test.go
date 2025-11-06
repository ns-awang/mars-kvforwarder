package aggregator

import (
    "testing"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/netSkope/mars-lib/src/log"
    "github.com/stretchr/testify/require"
    obs "mars-kvforwarder/internal/observability"
)

func TestMetricsCountersIncrementOnFlush(t *testing.T) {
    // Use isolated registry and initialize metrics before creating batcher
    reg := prometheus.NewRegistry()
    obs.ResetForTest()
    obs.Init(reg)

    _ = log.GetLogger() // ensure logger init
    b := NewAggregator(2)
    b.Begin("t")
    _ = b.ApplyChange("t", KVChange{Key: "a", Value: []byte("1")})
    out := b.ApplyChange("t", KVChange{Key: "b", Value: []byte("2")})
    require.Len(t, out, 1)

    // Gather metrics and ensure counters moved
    mfs, err := reg.Gather()
    require.NoError(t, err)

    var batches, items float64
    for _, mf := range mfs {
        if mf.GetName() == "kvforwarder_batches_total" {
            if len(mf.Metric) > 0 && mf.Metric[0].Counter != nil {
                batches = mf.Metric[0].Counter.GetValue()
            }
        }
        if mf.GetName() == "kvforwarder_items_total" {
            if len(mf.Metric) > 0 && mf.Metric[0].Counter != nil {
                items = mf.Metric[0].Counter.GetValue()
            }
        }
    }
    require.GreaterOrEqual(t, batches, 1.0)
    require.GreaterOrEqual(t, items, 2.0)
}



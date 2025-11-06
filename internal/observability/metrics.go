package observability

import (
    "sync"

    "github.com/prometheus/client_golang/prometheus"
)

var (
    initOnce        sync.Once
    batchesTotal    prometheus.Counter
    itemsTotal      prometheus.Counter
    flushLatencySec prometheus.Histogram
    errorsTotal     prometheus.Counter
)

// Init registers metrics with the provided registerer. If reg is nil,
// prometheus.DefaultRegisterer is used. Safe to call multiple times.
func Init(reg prometheus.Registerer) {
    if reg == nil {
        reg = prometheus.DefaultRegisterer
    }
    initOnce.Do(func() {
        batchesTotal = prometheus.NewCounter(prometheus.CounterOpts{
            Name: "kvforwarder_batches_total",
            Help: "Total number of emitted batches",
        })
        itemsTotal = prometheus.NewCounter(prometheus.CounterOpts{
            Name: "kvforwarder_items_total",
            Help: "Total number of KV items emitted across all batches",
        })
        flushLatencySec = prometheus.NewHistogram(prometheus.HistogramOpts{
            Name:    "kvforwarder_flush_latency_seconds",
            Help:    "Latency to build and emit a batch",
            Buckets: prometheus.DefBuckets,
        })
        errorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
            Name: "kvforwarder_errors_total",
            Help: "Total number of errors encountered",
        })
        reg.MustRegister(batchesTotal, itemsTotal, flushLatencySec, errorsTotal)
    })
}

// ObserveFlush records a flush with count and latency seconds.
func ObserveFlush(nItems int, seconds float64) {
    if flushLatencySec != nil {
        flushLatencySec.Observe(seconds)
    }
    if batchesTotal != nil {
        batchesTotal.Inc()
    }
    if itemsTotal != nil {
        itemsTotal.Add(float64(nItems))
    }
}

// IncError increments the error counter.
func IncError() {
    if errorsTotal != nil {
        errorsTotal.Inc()
    }
}

// ResetForTest resets metric globals so tests can reinitialize with
// a custom registry. Do not use in production code.
func ResetForTest() {
    batchesTotal = nil
    itemsTotal = nil
    flushLatencySec = nil
    errorsTotal = nil
    initOnce = sync.Once{}
}



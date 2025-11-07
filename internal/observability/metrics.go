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
	processedRows   prometheus.Counter
	parseErrors     prometheus.Counter
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
		processedRows = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kvforwarder_processed_rows_total",
			Help: "Total number of binlog rows processed",
		})
		parseErrors = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kvforwarder_parse_errors_total",
			Help: "Total number of binlog parse/mapping errors",
		})
		reg.MustRegister(batchesTotal, itemsTotal, flushLatencySec, errorsTotal, processedRows, parseErrors)
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

// IncProcessedRow increments processed rows counter.
func IncProcessedRow() {
	if processedRows != nil {
		processedRows.Inc()
	}
}

// IncParseError increments parse/mapping error counter.
func IncParseError() {
	if parseErrors != nil {
		parseErrors.Inc()
	}
}

// No retry/fatal-stop metrics per current requirements

// ResetForTest resets metric globals so tests can reinitialize with
// a custom registry. Do not use in production code.
func ResetForTest() {
	batchesTotal = nil
	itemsTotal = nil
	flushLatencySec = nil
	errorsTotal = nil
	processedRows = nil
	parseErrors = nil
	initOnce = sync.Once{}
}

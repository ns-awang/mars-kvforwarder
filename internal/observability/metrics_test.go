package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMetricsCountersIncrementOnFlush(t *testing.T) {
	// Use isolated registry and initialize metrics before creating aggregator
	reg := prometheus.NewRegistry()
	ResetForTest()
	Init(reg)

	// Directly exercise observability API without importing aggregator
	ObserveFlush(2, 0.01)
	ObserveFlush(1, 0.02)
	IncError()

	// Gather metrics and ensure counters moved
	mfs, err := reg.Gather()
	require.NoError(t, err)

	var batches, items, errors float64
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
		if mf.GetName() == "kvforwarder_errors_total" {
			if len(mf.Metric) > 0 && mf.Metric[0].Counter != nil {
				errors = mf.Metric[0].Counter.GetValue()
			}
		}
	}
	require.GreaterOrEqual(t, batches, 2.0)
	require.GreaterOrEqual(t, items, 3.0)
	require.GreaterOrEqual(t, errors, 1.0)
}

func TestBinlogMetricCounters(t *testing.T) {
	reg := prometheus.NewRegistry()
	ResetForTest()
	Init(reg)

	// Exercise binlog-oriented counters
	IncProcessedRow()
	IncProcessedRow()
	IncParseError()

	mfs, err := reg.Gather()
	require.NoError(t, err)

	var rows, parseErrs float64
	for _, mf := range mfs {
		switch mf.GetName() {
		case "kvforwarder_processed_rows_total":
			if len(mf.Metric) > 0 && mf.Metric[0].Counter != nil {
				rows = mf.Metric[0].Counter.GetValue()
			}
		case "kvforwarder_parse_errors_total":
			if len(mf.Metric) > 0 && mf.Metric[0].Counter != nil {
				parseErrs = mf.Metric[0].Counter.GetValue()
			}
		}
	}
	require.Equal(t, 2.0, rows)
	require.Equal(t, 1.0, parseErrs)
}

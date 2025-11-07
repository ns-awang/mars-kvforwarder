package binlog

import (
	"context"
	"time"

	obs "mars-kvforwarder/internal/observability"

	mlog "github.com/netSkope/mars-lib/src/log"
)

// RowEvent represents a single row parsed from the binlog.
// Implementation of the parser will define the full shape; the streamer treats it as opaque.
type RowEvent struct{}

// Source produces RowEvents from the binlog.
type Source interface {
	Next(ctx context.Context) (RowEvent, error)
}

// TransientError marks an error as retryable.
type TransientError struct{ Err error }

func (e TransientError) Error() string { return e.Err.Error() }

// StreamBinlog consumes events from src and invokes sink for each event.
// Retries transient errors up to retryLimit with exponential backoff; stops on fatal errors.
func StreamBinlog(ctx context.Context, src Source, sink func(RowEvent)) error {
	const (
		retryLimit     = 3
		initialBackoff = 100 * time.Millisecond
		maxBackoff     = 3 * time.Second
	)
	log := mlog.GetLogger()
	backoff := initialBackoff
	retries := 0
	for {
		if ctx.Err() != nil {
			return nil
		}

		ev, err := src.Next(ctx)
		if err == nil {
			retries = 0
			backoff = initialBackoff
			sink(ev)
			continue
		}

		// fatal (non-transient) → early return
		if _, ok := err.(TransientError); !ok {
			obs.IncError()
			log.Errorf("binlog fatal error: %v", err)
			return err
		}

		// transient handling (flat, with early returns on stop conditions)
		retries++
		obs.IncError()
		log.Warnf("binlog transient error (attempt %d/%d): %v", retries, retryLimit, err)
		if retries > retryLimit {
			log.Errorf("binlog transient error exceeded retries: %v", err)
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
		continue
	}
}

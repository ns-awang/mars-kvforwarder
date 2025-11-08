package binlog

import (
	"context"
	"errors"
	"time"

	"mars-kvforwarder/internal/config"
	obs "mars-kvforwarder/internal/observability"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mlog "github.com/netSkope/mars-lib/src/log"
)

const (
	defaultInitialBackoff = 100 * time.Millisecond
	defaultMaxBackoff     = 3 * time.Second
	defaultHeartbeat      = 2 * time.Second
	defaultReadTimeout    = 1 * time.Second
)

var (
	connLog = mlog.GetLogger()

	connectFunc = realConnect
)

// ConnectOptions controls connection parameters beyond the base config.
type ConnectOptions struct {
	ServerID       uint32
	Flavor         string
	GTID           mysql.GTIDSet
	Heartbeat      time.Duration
	ReadTimeout    time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// ConnectWithRetry attempts to establish a binlog stream using exponential backoff.
func ConnectWithRetry(ctx context.Context, cfg config.MySQLConfig, opts ConnectOptions) (BinlogEventSource, func() error, error) {
	if opts.InitialBackoff <= 0 {
		opts.InitialBackoff = defaultInitialBackoff
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = defaultMaxBackoff
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = defaultHeartbeat
	}
	if opts.ReadTimeout <= 0 {
		opts.ReadTimeout = defaultReadTimeout
	}
	if opts.ServerID == 0 {
		opts.ServerID = 1001
	}
	if opts.Flavor == "" {
		opts.Flavor = "mysql"
	}

	backoff := opts.InitialBackoff

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		src, cleanup, err := connectFunc(ctx, cfg, opts)
		if err == nil {
			connLog.Infof("mysql binlog connected: database=%s host=%s attempt=%d", cfg.Database, cfg.Host, attempt)
			return src, cleanup, nil
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, nil, err
		}

		obs.IncError()
		if !isRetryable(err) {
			connLog.Errorf("mysql binlog connection fatal: database=%s host=%s err=%v", cfg.Database, cfg.Host, err)
			return nil, nil, err
		}

		connLog.Warnf("mysql binlog connection retry %d: database=%s host=%s err=%v backoff=%s", attempt, cfg.Database, cfg.Host, err, backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
		if backoff < opts.MaxBackoff {
			backoff *= 2
			if backoff > opts.MaxBackoff {
				backoff = opts.MaxBackoff
			}
		}
	}
}

func isRetryable(err error) bool {
	var r retryableError
	if errors.As(err, &r) {
		return true
	}
	// Default to retryable for unknown errors (e.g., network) so caller can cap attempts.
	return !errors.Is(err, ErrNonRetryable)
}

// ErrNonRetryable signals that ConnectWithRetry should not retry.
var ErrNonRetryable = errors.New("non-retryable connection error")

type retryableError struct {
	err error
}

func (e retryableError) Error() string {
	return e.err.Error()
}

func realConnect(ctx context.Context, cfg config.MySQLConfig, opts ConnectOptions) (BinlogEventSource, func() error, error) {
	syncCfg := replication.BinlogSyncerConfig{
		ServerID:        opts.ServerID,
		Flavor:          opts.Flavor,
		Host:            cfg.Host,
		Port:            uint16(cfg.Port),
		User:            cfg.User,
		Password:        cfg.Password,
		HeartbeatPeriod: opts.Heartbeat,
		ReadTimeout:     opts.ReadTimeout,
		ParseTime:       true,
		UseDecimal:      true,
	}

	syncer := replication.NewBinlogSyncer(syncCfg)

	var (
		streamer *replication.BinlogStreamer
		err      error
	)
	if opts.GTID != nil {
		streamer, err = syncer.StartSyncGTID(opts.GTID)
	} else {
		streamer, err = syncer.StartSync(mysql.Position{})
	}
	if err != nil {
		syncer.Close()
		return nil, nil, retryableError{err: err}
	}

	source := &syncSource{streamer: streamer}
	cleanup := func() error {
		syncer.Close()
		return nil
	}
	return source, cleanup, nil
}

type syncSource struct {
	streamer *replication.BinlogStreamer
}

func (s *syncSource) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	ev, err := s.streamer.GetEvent(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, TransientError{Err: err}
	}
	return ev, nil
}

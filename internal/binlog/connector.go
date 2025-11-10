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
	heartbeatPeriod = 2 * time.Second
	readTimeout     = 1 * time.Second
	serverID        = 1001
	flavor          = "mysql"

	initialBackoff = 100 * time.Millisecond
	maxRetries     = 6
)

var (
	logger = mlog.GetLogger()

	connect = connectDB
)

// ConnectWithRetry attempts to establish a binlog stream using exponential backoff until success or max retry duration.
func ConnectWithRetry(ctx context.Context, cfg config.MySQLConfig, gtid mysql.GTIDSet) (src BinlogEventSource, cleanup func() error, err error) {
	backoff := initialBackoff

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err = ctx.Err(); err != nil {
			return
		}

		src, cleanup, err = connect(cfg, gtid)
		if err == nil {
			logger.Infof("mysql binlog connected: database=%s host=%s attempt=%d", cfg.Database, cfg.Host, attempt)
			return
		}

		obs.IncError()
		if !isRetryable(err) || attempt == maxRetries {
			logger.Errorf("mysql binlog connection failed err=%v", err)
			return
		}

		logger.Warnf("mysql binlog connection retry %d: database=%s host=%s err=%v backoff=%s", attempt, cfg.Database, cfg.Host, err, backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			if err = ctx.Err(); err == nil {
				err = errors.New("context canceled")
			}
			return
		case <-timer.C:
			timer.Stop()
		}
		backoff *= 2
	}
	return
}

func connectDB(cfg config.MySQLConfig, gtid mysql.GTIDSet) (BinlogEventSource, func() error, error) {
	syncCfg := replication.BinlogSyncerConfig{
		ServerID:        serverID,
		Flavor:          flavor,
		Host:            cfg.Host,
		Port:            uint16(cfg.Port),
		User:            cfg.User,
		Password:        cfg.Password,
		HeartbeatPeriod: heartbeatPeriod,
		ReadTimeout:     readTimeout,
		ParseTime:       true,
		UseDecimal:      true,
	}

	syncer := replication.NewBinlogSyncer(syncCfg)

	var (
		streamer *replication.BinlogStreamer
		err      error
	)
	if gtid != nil {
		streamer, err = syncer.StartSyncGTID(gtid)
	} else {
		streamer, err = syncer.StartSync(mysql.Position{})
	}
	if err != nil {
		syncer.Close()
		return nil, nil, err
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

var ErrNonRetryable = errors.New("non-retryable connection error")

func isRetryable(err error) bool {
	return !errors.Is(err, ErrNonRetryable)
}

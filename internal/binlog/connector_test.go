package binlog

import (
	"context"
	"errors"
	"testing"
	"time"

	"mars-kvforwarder/internal/config"
	obs "mars-kvforwarder/internal/observability"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
)

type fakeSource struct{}

func (f *fakeSource) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	return nil, context.Canceled
}

func TestConnectWithRetrySuccessAfterTransientFailures(t *testing.T) {
	attempts := 0
	fake := &fakeSource{}
	origConnect := connect
	t.Cleanup(func() { connect = origConnect })

	connect = func(cfg config.MySQLConfig, gtid mysql.GTIDSet) (BinlogEventSource, func() error, error) {
		attempts++
		if attempts < 3 {
			return nil, nil, errors.New("temp failure")
		}
		return fake, func() error { return nil }, nil
	}

	obs.ResetForTest()

	cfg := config.MySQLConfig{
		Host:     "mysql-primary.internal",
		User:     "kvforwarder",
		Password: "secret",
		Database: config.DefaultDatabase,
		Port:     config.MySQLPort,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	src, cleanup, err := ConnectWithRetry(ctx, cfg, nil)
	assert.NoError(t, err)
	assert.Equal(t, fake, src)
	assert.NotNil(t, cleanup)
	assert.Equal(t, 3, attempts)
}

func TestConnectWithRetryStopsOnFatalError(t *testing.T) {
	attempts := 0
	origConnect := connect
	t.Cleanup(func() { connect = origConnect })

	connect = func(cfg config.MySQLConfig, gtid mysql.GTIDSet) (BinlogEventSource, func() error, error) {
		attempts++
		return nil, nil, ErrNonRetryable
	}

	obs.ResetForTest()

	cfg := config.MySQLConfig{Host: "mysql-primary", User: "kvf", Password: "secret", Database: config.DefaultDatabase, Port: config.MySQLPort}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, _, err := ConnectWithRetry(ctx, cfg, nil)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrNonRetryable))
	assert.Equal(t, 1, attempts)
}

func TestConnectWithRetryRespectsContextCancellation(t *testing.T) {
	attempts := 0
	done := make(chan struct{})
	origConnect := connect
	t.Cleanup(func() { connect = origConnect })

	connect = func(cfg config.MySQLConfig, gtid mysql.GTIDSet) (BinlogEventSource, func() error, error) {
		attempts++
		if attempts == 1 {
			close(done)
		}
		return nil, nil, errors.New("temp failure")
	}

	obs.ResetForTest()

	cfg := config.MySQLConfig{Host: "mysql-primary", User: "kvf", Password: "secret", Database: config.DefaultDatabase, Port: config.MySQLPort}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-done
		time.Sleep(2 * time.Millisecond)
		cancel()
	}()

	_, _, err := ConnectWithRetry(ctx, cfg, nil)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
	assert.GreaterOrEqual(t, attempts, 1)
}

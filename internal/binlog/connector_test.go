package binlog

import (
	"context"
	"errors"
	"testing"
	"time"

	"mars-kvforwarder/internal/config"
	obs "mars-kvforwarder/internal/observability"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

type fakeSource struct{}

func (f *fakeSource) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	return nil, context.Canceled
}

func TestConnectWithRetrySuccessAfterTransientFailures(t *testing.T) {
	orig := connectFunc
	t.Cleanup(func() { connectFunc = orig })

	attempts := 0
	fake := &fakeSource{}
	connectFunc = func(ctx context.Context, cfg config.MySQLConfig, opts ConnectOptions) (BinlogEventSource, func() error, error) {
		attempts++
		if attempts < 3 {
			return nil, nil, retryableError{err: errors.New("temp failure")}
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
	opts := ConnectOptions{
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	src, cleanup, err := ConnectWithRetry(ctx, cfg, opts)
	require.NoError(t, err)
	require.Equal(t, fake, src)
	require.NotNil(t, cleanup)
	require.Equal(t, 3, attempts)
}

func TestConnectWithRetryStopsOnFatalError(t *testing.T) {
	orig := connectFunc
	t.Cleanup(func() { connectFunc = orig })

	attempts := 0
	connectFunc = func(ctx context.Context, cfg config.MySQLConfig, opts ConnectOptions) (BinlogEventSource, func() error, error) {
		attempts++
		return nil, nil, ErrNonRetryable
	}

	obs.ResetForTest()

	cfg := config.MySQLConfig{Host: "mysql-primary", User: "kvf", Password: "secret", Database: config.DefaultDatabase, Port: config.MySQLPort}
	opts := ConnectOptions{InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, _, err := ConnectWithRetry(ctx, cfg, opts)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrNonRetryable))
	require.Equal(t, 1, attempts)
}

func TestConnectWithRetryRespectsContextCancellation(t *testing.T) {
	orig := connectFunc
	t.Cleanup(func() { connectFunc = orig })

	attempts := 0
	done := make(chan struct{})
	connectFunc = func(ctx context.Context, cfg config.MySQLConfig, opts ConnectOptions) (BinlogEventSource, func() error, error) {
		attempts++
		if attempts == 1 {
			close(done)
		}
		return nil, nil, retryableError{err: errors.New("temp failure")}
	}

	obs.ResetForTest()

	cfg := config.MySQLConfig{Host: "mysql-primary", User: "kvf", Password: "secret", Database: config.DefaultDatabase, Port: config.MySQLPort}
	opts := ConnectOptions{InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-done
		time.Sleep(2 * time.Millisecond)
		cancel()
	}()

	_, _, err := ConnectWithRetry(ctx, cfg, opts)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
	require.GreaterOrEqual(t, attempts, 1)
}

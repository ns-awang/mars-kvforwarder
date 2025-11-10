package binlog

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

func TestGTIDTrackerRecordAndLast(t *testing.T) {
	tracker := &GTIDTracker{}
	require.Nil(t, tracker.Last())

	ev1 := &replication.GTIDEvent{
		SID: decodeUUID(t, "a3d6d136-2de9-11ee-b7ac-0242ac120002"),
		GNO: 42,
	}
	require.NoError(t, tracker.Record("mysql", ev1))

	last := tracker.Last()
	require.NotNil(t, last)
	require.Equal(t, "a3d6d136-2de9-11ee-b7ac-0242ac120002:42", last.String())

	ev2 := &replication.GTIDEvent{
		SID: decodeUUID(t, "a3d6d136-2de9-11ee-b7ac-0242ac120002"),
		GNO: 100,
	}
	require.NoError(t, tracker.Record("mysql", ev2))

	last = tracker.Last()
	require.NotNil(t, last)
	require.Equal(t, "a3d6d136-2de9-11ee-b7ac-0242ac120002:100", last.String())

	// Ensure Clone semantics.
	last.String() // invoke method to ensure interface remains valid
}

func decodeUUID(t *testing.T, uuid string) []byte {
	t.Helper()
	clean := strings.ReplaceAll(uuid, "-", "")
	buf, err := hex.DecodeString(clean)
	require.NoError(t, err)
	return buf
}

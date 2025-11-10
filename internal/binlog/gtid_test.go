package binlog

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
)

func TestGTIDTrackerRecordAndLast(t *testing.T) {
	tracker := &GTIDTracker{}
	assert.Nil(t, tracker.Last())

	ev1 := &replication.GTIDEvent{
		SID: decodeUUID(t, "a3d6d136-2de9-11ee-b7ac-0242ac120002"),
		GNO: 42,
	}
	assert.NoError(t, tracker.Record("mysql", ev1))

	last := tracker.Last()
	if assert.NotNil(t, last) {
		assert.Equal(t, "a3d6d136-2de9-11ee-b7ac-0242ac120002:42", last.String())
	}

	ev2 := &replication.GTIDEvent{
		SID: decodeUUID(t, "a3d6d136-2de9-11ee-b7ac-0242ac120002"),
		GNO: 100,
	}
	assert.NoError(t, tracker.Record("mysql", ev2))

	last = tracker.Last()
	if assert.NotNil(t, last) {
		assert.Equal(t, "a3d6d136-2de9-11ee-b7ac-0242ac120002:100", last.String())
	}
}

func decodeUUID(t *testing.T, uuid string) []byte {
	t.Helper()
	clean := strings.ReplaceAll(uuid, "-", "")
	buf, err := hex.DecodeString(clean)
	assert.NoError(t, err)
	return buf
}

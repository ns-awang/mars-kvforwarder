package binlog

import (
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// GTIDTracker keeps the last observed GTID set for reconnect purposes.
type GTIDTracker struct {
	mu  sync.RWMutex
	set mysql.GTIDSet
}

// Record updates the tracker with the GTID contained in the given event.
func (t *GTIDTracker) Record(flavor string, ev *replication.GTIDEvent) error {
	uuid, err := formatUUID(ev.SID)
	if err != nil {
		return err
	}
	gtidStr := fmt.Sprintf("%s:%d", uuid, ev.GNO)
	set, err := mysql.ParseGTIDSet(flavor, gtidStr)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.set = set
	return nil
}

// Last returns a clone of the last recorded GTID set.
func (t *GTIDTracker) Last() mysql.GTIDSet {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.set == nil {
		return nil
	}
	return t.set.Clone()
}

func formatUUID(sid []byte) (string, error) {
	if len(sid) != 16 {
		return "", fmt.Errorf("invalid SID length: %d", len(sid))
	}
	buf := make([]byte, 36)
	hex.Encode(buf[0:8], sid[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], sid[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], sid[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], sid[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], sid[10:16])
	return string(buf), nil
}

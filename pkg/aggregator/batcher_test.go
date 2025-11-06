package aggregator

import (
	"testing"
)

func TestBatcherFlushOnMaxSize(t *testing.T) {
	b := NewBatcher(50)
	tx := "tx-10"
	b.Begin(tx)

	var emitted []Batch
	// Apply 50 unique keys
	for i := 0; i < 50; i++ {
		out := b.ApplyChange(tx, KVChange{Key: keyN(i), Value: []byte("v")})
		if i < 49 && len(out) != 0 {
			t.Fatalf("unexpected early flush at %d", i)
		}
		if i == 49 {
			if len(out) != 1 {
				t.Fatalf("expected one batch at threshold, got %d", len(out))
			}
			if out[0].BatchIndex != 1 || out[0].BatchTotal != 0 {
				t.Fatalf("expected index=1,total=0 mid-tx, got %d,%d", out[0].BatchIndex, out[0].BatchTotal)
			}
			emitted = append(emitted, out[0])
		}
	}

	// Add one more unique key
	out := b.ApplyChange(tx, KVChange{Key: "k-extra", Value: []byte("v")})
	if len(out) != 0 {
		t.Fatalf("did not expect flush before commit for 51st key")
	}

	// Commit should flush final batch with total=2
	out = b.Commit(tx)
	if len(out) != 1 {
		t.Fatalf("expected one final batch, got %d", len(out))
	}
	if out[0].BatchIndex != 2 || out[0].BatchTotal != 2 {
		t.Fatalf("expected final batch index=2,total=2, got %d,%d", out[0].BatchIndex, out[0].BatchTotal)
	}
	emitted = append(emitted, out[0])

	if len(emitted[0].Items) != 50 {
		t.Fatalf("first batch items=50 expected, got %d", len(emitted[0].Items))
	}
	if len(emitted[1].Items) != 1 {
		t.Fatalf("second batch items=1 expected, got %d", len(emitted[1].Items))
	}
}

func TestBatcherCommitOnly(t *testing.T) {
	b := NewBatcher(50)
	tx := "tx-11"
	b.Begin(tx)
	for i := 0; i < 10; i++ {
		b.ApplyChange(tx, KVChange{Key: keyN(i), Value: []byte("v")})
	}
	out := b.Commit(tx)
	if len(out) != 1 {
		t.Fatalf("expected one batch on commit, got %d", len(out))
	}
	if out[0].BatchIndex != 1 || out[0].BatchTotal != 1 {
		t.Fatalf("expected index=1,total=1, got %d,%d", out[0].BatchIndex, out[0].BatchTotal)
	}
}

func TestBatcherLastWriteWinsWithinWindow(t *testing.T) {
	b := NewBatcher(50)
	tx := "tx-12"
	b.Begin(tx)
	b.ApplyChange(tx, KVChange{Key: "k", Value: []byte("a"), Operation: OperationCreate})
	b.ApplyChange(tx, KVChange{Key: "k", Value: []byte("b"), Operation: OperationUpdate})
	out := b.Commit(tx)
	if len(out) != 1 {
		t.Fatalf("expected one batch, got %d", len(out))
	}
	if len(out[0].Items) != 1 {
		t.Fatalf("expected one item, got %d", len(out[0].Items))
	}
	if string(out[0].Items[0].Value) != "b" {
		t.Fatalf("expected last-write value 'b', got %q", string(out[0].Items[0].Value))
	}
}

// keyN returns a string key for integer n.
func keyN(n int) string {
	return "k-" + strconvItoa(n)
}

// Small local itoa to avoid importing strconv in multiple tests.
func strconvItoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + (n % 10))
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

package aggregator

import (
	"bytes"
	"testing"
)

func TestLastWriteWinsWithinTransaction(t *testing.T) {
	a := New()
	tx := "tx-1"
	if err := a.Begin(tx); err != nil {
		t.Fatalf("begin failed: %v", err)
	}

	// Same key updated multiple times; last write should win
	if err := a.ApplyChange(tx, KVChange{Key: "k1", Value: []byte("1"), Operation: OperationCreate}); err != nil {
		t.Fatalf("apply 1 failed: %v", err)
	}
	if err := a.ApplyChange(tx, KVChange{Key: "k1", Value: []byte("2"), Operation: OperationUpdate}); err != nil {
		t.Fatalf("apply 2 failed: %v", err)
	}
	if err := a.ApplyChange(tx, KVChange{Key: "k1", Value: []byte("3"), Operation: OperationUpdate}); err != nil {
		t.Fatalf("apply 3 failed: %v", err)
	}

	agg, err := a.Commit(tx)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if len(agg.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(agg.Items))
	}
	if !bytes.Equal(agg.Items[0].Value, []byte("3")) {
		t.Fatalf("expected last write value '3', got %q", string(agg.Items[0].Value))
	}
}

func TestMultipleKeysDeterministicOrder(t *testing.T) {
	a := New()
	tx := "tx-2"
	if err := a.Begin(tx); err != nil {
		t.Fatalf("begin failed: %v", err)
	}

	if err := a.ApplyChange(tx, KVChange{Key: "a", Value: []byte("x=1"), Operation: OperationCreate}); err != nil {
		t.Fatalf("apply a failed: %v", err)
	}
	if err := a.ApplyChange(tx, KVChange{Key: "b", Value: []byte("y=2"), Operation: OperationUpdate}); err != nil {
		t.Fatalf("apply b failed: %v", err)
	}
	// Update key a again; should remain first in ordering
	if err := a.ApplyChange(tx, KVChange{Key: "a", Value: []byte("x=9"), Operation: OperationUpdate}); err != nil {
		t.Fatalf("apply a2 failed: %v", err)
	}

	agg, err := a.Commit(tx)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if len(agg.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(agg.Items))
	}
	if agg.Items[0].Key != "a" || agg.Items[1].Key != "b" {
		t.Fatalf("unexpected order: %+v", agg.Items)
	}
	if !bytes.Equal(agg.Items[0].Value, []byte("x=9")) {
		t.Fatalf("expected a value 'x=9', got %q", string(agg.Items[0].Value))
	}
}

func TestBeginTwiceError(t *testing.T) {
	a := New()
	tx := "tx-3"
	if err := a.Begin(tx); err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if err := a.Begin(tx); err == nil {
		t.Fatalf("expected error on second begin")
	}
}

func TestApplyWithoutBeginError(t *testing.T) {
	a := New()
	tx := "tx-4"
	if err := a.ApplyChange(tx, KVChange{Key: "k", Value: []byte("1"), Operation: OperationCreate}); err == nil {
		t.Fatalf("expected error applying without begin")
	}
}

func TestAbortRemovesBuffer(t *testing.T) {
	a := New()
	tx := "tx-5"
	if err := a.Begin(tx); err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if a.PendingTxCount() != 1 {
		t.Fatalf("expected 1 pending, got %d", a.PendingTxCount())
	}
	if err := a.Abort(tx); err != nil {
		t.Fatalf("abort failed: %v", err)
	}
	if a.PendingTxCount() != 0 {
		t.Fatalf("expected 0 pending after abort, got %d", a.PendingTxCount())
	}
}

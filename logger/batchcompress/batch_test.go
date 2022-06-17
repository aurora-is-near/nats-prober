package batchcompress

import (
	"testing"
	"time"
)

func TestBatch(t *testing.T) {
	var batched bool
	rec := func(d []byte) {
		batched = true
	}
	b := NewBatcher(100, time.Second, rec)
	b.Add(make([]byte, 99))
	if batched {
		t.Error("Early batching")
	}
	b.Add(make([]byte, 10))
	if !batched {
		t.Error("No size enforced")
	}
}

func TestBatchTime(t *testing.T) {
	var batched bool
	rec := func(d []byte) {
		batched = true
	}
	b := NewBatcher(100, time.Second, rec)
	b.Add(make([]byte, 99))
	time.Sleep(time.Second + time.Millisecond*100)
	if !batched {
		t.Error("No time enforced")
	}
}

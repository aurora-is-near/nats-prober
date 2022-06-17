package gapstore

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/aurora-is-near/nats-prober/logger/freelist"
)

func randBytes(size int) []byte {
	r := make([]byte, size)
	_, _ = io.ReadFull(rand.Reader, r)
	return r
}

func TestSlot(t *testing.T) {
	elements := 100
	td1 := []byte("something")
	td2 := []byte("something else")
	td3 := []byte("something else entirely")
	list := freelist.New(elements)
	slots := New(elements)
	slot1 := slots.Write(td1, list.Draw)
	slot2 := slots.Write(td2, list.Draw)
	slot3 := slots.Write(td3, list.Draw)
	if !bytes.Equal(slots.Read(slot1), td1) {
		t.Fatal("Simple read&write 1")
	}
	if !bytes.Equal(slots.Read(slot2), td2) {
		t.Fatal("Simple read&write 2")
	}
	if !bytes.Equal(slots.Read(slot3), td3) {
		t.Fatal("Simple read&write 3")
	}
	if list.Free() != elements-3 {
		t.Fatal("Freelist not consumed")
	}
	if !bytes.Equal(slots.ReadOnce(slot1, list.Return), td1) || !bytes.Equal(slots.ReadOnce(slot2, list.Return), td2) || !bytes.Equal(slots.ReadOnce(slot3, list.Return), td3) {
		t.Fatal("ReadOnce")
	}
	if list.Free() != elements {
		t.Fatal("Freelist not returned")
	}
}

func TestSlotFuzz(t *testing.T) {
	elements := MaxPayload * 6
	sizes := MaxPayload * 3
	td := make([][]byte, sizes)
	entries := make([]uint64, sizes)
	list := freelist.New(elements)
	slots := New(elements)
	free := list.Free()
	for i := 0; i < sizes; i++ {
		td[i] = randBytes(i + 1)
		entries[i] = slots.Write(td[i], list.Draw)
		if freeNew := list.Free(); freeNew >= free {
			t.Fatalf("Freelist not consumed: %d, %d>=%d", i, freeNew, free)
		} else {
			free = freeNew
		}
	}
	free = list.Free()
	for i := 0; i < sizes; i++ {
		if !bytes.Equal(td[i], slots.ReadOnce(entries[i], list.Return)) {
			t.Fatalf("ReadWrite: %d", i)
		}
		if freeNew := list.Free(); freeNew <= free {
			t.Fatal("Freelist not returned")
		} else {
			free = freeNew
		}
	}
}

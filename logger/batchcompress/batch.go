package batchcompress

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"
	"time"
)

const (
	dataSegments = 100
)

type IntegerType interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64
}

func min[T IntegerType](a, b T) T {
	if a < b {
		return a
	}
	return b
}

var (
	nl = []byte("\n")
)

// Batch is a size constrained batch.
type Batch struct {
	maxSize int
	curSize int
	data    [][]byte
}

// NewBatch creates a size constrained batch.
func NewBatch(maxSize int) *Batch {
	r := &Batch{
		maxSize: maxSize,
		data:    make([][]byte, 0, dataSegments),
	}
	return r
}

// Add message to batch. Returns false if new message makes batch too big.
func (batch *Batch) Add(d []byte) bool {
	l := len(d)
	if batch.curSize+l > batch.maxSize {
		return false
	}
	batch.curSize += l
	batch.data = append(batch.data, d)
	return true
}

// Flush compresses the batch and writes it to w.
func (batch *Batch) Flush(w io.Writer) {
	g := gzip.NewWriter(w)
	for i := 0; i < len(batch.data) && len(batch.data) > 0; i++ {
		_, _ = g.Write(batch.data[i])
		batch.data[i] = nil
		_, _ = g.Write(nl)
	}
	_ = g.Flush()
	_ = g.Close()
}

// Batcher is a time and size constrained batch generator.
type Batcher struct {
	maxAge       time.Duration
	maxSize      int
	currentBatch *Batch

	t *time.Timer
	m *sync.Mutex
	r func([]byte)
}

// NewBatcher creates a new batcher that flushes after maxSize or maxAge are reached. Calls receiver with the result.
func NewBatcher(maxSize int, maxAge time.Duration, receiver func([]byte)) *Batcher {
	r := &Batcher{
		maxAge:       maxAge,
		maxSize:      maxSize,
		currentBatch: NewBatch(maxSize),
		m:            new(sync.Mutex),
		r:            receiver,
	}
	r.t = time.AfterFunc(maxAge, r.Flush)
	return r
}

// Add message to batcher.
func (batcher *Batcher) Add(d []byte) {
	batcher.m.Lock()
	defer batcher.m.Unlock()
	if batcher.currentBatch.Add(d) {
		return
	}
	batcher.flush()
	_ = batcher.currentBatch.Add(d[:min(batcher.maxSize, len(d)-1)])
}

func (batcher *Batcher) flush() {
	if !batcher.t.Stop() {
		go func() { <-batcher.t.C }()
		batcher.t.Reset(batcher.maxAge)
	}
	if len(batcher.currentBatch.data) == 0 {
		return
	}
	oldBatch := batcher.currentBatch
	batcher.currentBatch = NewBatch(batcher.maxSize)
	a := new(bytes.Buffer)
	oldBatch.Flush(a)
	batcher.r(a.Bytes())
}

// Flush the batch: Write output forcefully.
func (batcher *Batcher) Flush() {
	batcher.m.Lock()
	defer batcher.m.Unlock()
	batcher.flush()
}

// Close the batch processing and flush current content..
func (batcher *Batcher) Close() {
	if !batcher.t.Stop() {
		go func() { <-batcher.t.C }()
	}
	a := new(bytes.Buffer)
	batcher.currentBatch.Flush(a)
	batcher.r(a.Bytes())
}

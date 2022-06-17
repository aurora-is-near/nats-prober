// Package delayqueue implements a storage backed delayed queue.
package delayqueue

import (
	"os"
	"path"
	"sync"
	"time"

	"github.com/aurora-is-near/nats-prober/logger/freelist"
	"github.com/aurora-is-near/nats-prober/logger/gapstore"
	"github.com/aurora-is-near/nats-prober/logger/index"
	mmap "github.com/edsrzf/mmap-go"
)

const (
	// DefaultElements are the default number of elements that can be added to the delay queue
	DefaultElements = 1000
	// StorageFile is the name of the file that will be used for persistence.
	StorageFile     = "delayStorage.db"
	slotMargin      = 1
	minFireDuration = time.Millisecond * 100

	dumpCmd = 1
)

type IntegerType interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | time.Duration
}

type CallbackFunc func([]byte)

// DelayQueue implements a delayed queue. Elements added to it will be removed again after a specified duration, calling a callback in the process.
type DelayQueue struct {
	ring  index.IndexField
	slots gapstore.SlotStorage
	free  *freelist.Freelist

	delay  time.Duration
	submit CallbackFunc
	dump   CallbackFunc

	c     chan interface{}
	close chan chan struct{}

	mapFile *os.File
	mmap    mmap.MMap

	m *sync.Mutex
}

// Stop the delay queue.
func (queue *DelayQueue) Stop() {
	if queue == nil {
		return
	}
	queue.m.Lock()
	defer queue.m.Unlock()
	if queue.close != nil {
		q := make(chan struct{}, 2)
		queue.close <- q
		<-q
		close(queue.close)
		close(queue.c)
		close(q)
		queue.close = nil
		queue.c = nil
	}
	if queue.mmap != nil {
		_ = queue.mmap.Flush()
		_ = queue.mmap.Unmap()
		_ = queue.mapFile.Close()
		queue.mmap = nil
	}
}

func newQueue(delay time.Duration, elements int, file string) (*DelayQueue, error) {
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}
	ringSize := index.Size(elements)
	gapSize := gapstore.Size(elements + slotMargin)
	if err := f.Truncate(int64(ringSize + gapSize)); err != nil {
		_ = f.Close()
		return nil, err
	}

	mmapped, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	r := &DelayQueue{
		mmap:    mmapped,
		delay:   delay,
		mapFile: f,
		free:    freelist.New(elements + slotMargin),
		ring:    index.IndexField(mmapped[0:ringSize]),
		slots:   gapstore.SlotStorage(mmapped[ringSize : ringSize+gapSize]),
		m:       new(sync.Mutex),
	}
	return r, nil
}

// NewReadable creates delay queue that can be exported but should not be written to.
func NewReadable(file string) (*DelayQueue, error) {
	s, err := os.Stat(file)
	if err != nil {
		return nil, err
	}
	elements := (s.Size() - index.HeaderSize - slotMargin*gapstore.SlotSize) / (index.IndexSize + gapstore.SlotSize)
	return newQueue(0, int(elements), file)
}

// NewWritable creates a new delay queue. <delay> is the duration between adding and removing an element.
// <elements> is the number of elements the queue can contain at modest. <file> is the path to the file used for persistence/memory extension.
// <submitFunc> is the function that is called with the element that is removed. <dumpFunc> is the function to be called when a dump is requested.
func NewWriteable(delay time.Duration, elements int, file string, submitFunc, dumpFunc CallbackFunc) (*DelayQueue, error) {
	q, err := newQueue(delay, elements, file)
	if err != nil {
		return nil, err
	}
	q.c = make(chan interface{}, 100)
	q.close = make(chan chan struct{}, 2)
	q.submit = submitFunc
	q.dump = dumpFunc
	go q.loop()
	return q, nil
}

// Init is a helper function to set up a delay queue.
func Init(storageDir string, delayInSeconds int, submitFunc, dumpFunc func([]byte)) (*DelayQueue, error) {
	return NewWriteable(time.Duration(delayInSeconds)*time.Second, DefaultElements, path.Join(storageDir, StorageFile), submitFunc, dumpFunc)
}

// AddMsg adds a message to the delay queue.
func (queue *DelayQueue) AddMsg(d []byte) {
	if queue == nil {
		return
	}
	queue.c <- d
}

// Dump all messages from the queue to the callback.
func (queue *DelayQueue) Dump() {
	queue.c <- int(dumpCmd)
}

// Export the contents of a queue.
func (queue *DelayQueue) Export(callback func(time.Time, []byte)) {
	for i, _ := queue.ring.First(0); i <= queue.ring.MaxCounter(); i++ {
		e := queue.ring.At(i)
		callback(e.UnixTime().Time(), queue.slots.Read(e.BeginByte()))
	}
}

func max[T IntegerType](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func (queue *DelayQueue) receive(d []byte) bool {
	pos := queue.slots.Write(d, queue.free.Draw)
	if pos == gapstore.EndOfSlots {
		return false
	}
	if oldPos, _, replaced := queue.ring.Append(pos); replaced {
		queue.submit(queue.slots.ReadOnce(oldPos, queue.free.Return))
	}
	return true
}

func (queue *DelayQueue) sendDelayed(lastPos uint64) (uint64, bool) {
	change := false
MessageLoop:
	for {
		nextPos, found := queue.ring.First(lastPos)
		if found && queue.ring.At(nextPos).UnixTime().Time().Add(queue.delay).Before(time.Now()) {
			change = true
			queue.submit(queue.slots.ReadOnce(queue.ring.At(nextPos).BeginByte(), queue.free.Return))
			queue.ring.Wipe(nextPos)
			lastPos = nextPos
		} else {
			break MessageLoop
		}
	}
	return lastPos, change
}

func (queue *DelayQueue) loop() {
	var timer *time.Timer
	var nextTrigger index.UnixTimestamp
	var lastPos uint64
	var dumpStart, dumpEnd uint64
	var doDump bool
	timer = time.NewTimer(time.Hour * 1000)
	for {
		change := false
		select {
		case q := <-queue.close:
			q <- struct{}{}
			return
		case inf := <-queue.c:
			switch d := inf.(type) {
			case []byte:
				if !queue.receive(d) {
					continue
				}
				change = true
			case int:
				if d == dumpCmd && !doDump {
					start, found := queue.ring.First(0)
					if found {
						dumpStart, dumpEnd = start, queue.ring.MaxCounter()
					}
					doDump = true
				}
			default:
			}
		case <-timer.C:
			lastPos, change = queue.sendDelayed(lastPos)
		default:
			if doDump {
				queue.dump(queue.slots.Read(queue.ring.At(dumpStart).BeginByte()))
				dumpStart++
				if dumpStart > dumpEnd {
					doDump = false
					dumpStart, dumpEnd = 0, 0
				}
			}
		}
		if change {
			timePos, found := queue.ring.First(lastPos)
			if found {
				newTime := queue.ring.At(timePos).UnixTime()
				nextFire := max(newTime.Time().Add(queue.delay).Sub(time.Now()), minFireDuration)
				if newTime != nextTrigger {
					timer.Stop()
					timer.Reset(nextFire)
					nextTrigger = newTime
				}
			}
		}
	}
}

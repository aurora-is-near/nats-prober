package gapstore

import (
	"encoding/binary"
	"math"
)

const (
	SlotSize = 512
	MaxSlots = 1000

	EndOfSlots = math.MaxUint64

	numSize       = 8
	nextSlotBegin = 0
	nextSlotEnd   = nextSlotBegin + numSize
	lengthBegin   = nextSlotEnd
	lengthEnd     = lengthBegin + numSize
	headerSize    = lengthEnd

	MaxPayload = SlotSize - headerSize
)

var zeros = [headerSize]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

type Slot [SlotSize]byte

type IntegerType interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64
}

var decodeFunc = binary.BigEndian.Uint64
var encodeFunc = binary.BigEndian.PutUint64

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (slot *Slot) Write(nextSlotID uint64, data []byte) []byte {
	chunk := min(MaxPayload, len(data))
	encodeFunc(slot[lengthBegin:lengthEnd], uint64(chunk))
	copy(slot[headerSize:], data)
	slot.SetNext(nextSlotID)
	return data[chunk:]
}

func (slot *Slot) Delete() {
	copy(slot[:], zeros[:])
}

func (slot *Slot) SetNext(nextSlotID uint64) {
	encodeFunc(slot[nextSlotBegin:nextSlotEnd], nextSlotID)
}

func (slot *Slot) Read() ([]byte, uint64) {
	l := decodeFunc(slot[lengthBegin:lengthEnd])
	return slot[headerSize : headerSize+l], decodeFunc(slot[nextSlotBegin:nextSlotEnd])
}

type SlotStorage []byte

func Size[T IntegerType](elements T) int {
	return int(elements) * SlotSize
}

func New[T IntegerType](elements T) SlotStorage {
	return make([]byte, Size(elements))
}
func (storage SlotStorage) Len() int {
	return len(storage) / SlotSize
}

func (storage SlotStorage) At(pos uint64) *Slot {
	pos = pos % uint64(storage.Len())
	return (*Slot)(storage[pos*SlotSize : (1+pos)*SlotSize])
}

type ReturnBlockFunc func(uint64)

func (storage SlotStorage) read(pos uint64, returnFunc ReturnBlockFunc) []byte {
	r := make([]byte, 0, SlotSize)
	for i := 0; i < MaxSlots; i++ {
		slot := storage.At(pos)
		d, next := slot.Read()
		if returnFunc != nil {
			returnFunc(pos)
			slot.Delete()
		}
		r = append(r, d...)
		if next == EndOfSlots {
			return r
		}
		pos = next
	}
	return r
}

func (storage SlotStorage) Read(pos uint64) []byte {
	return storage.read(pos, nil)
}

func (storage SlotStorage) ReadOnce(pos uint64, returnFunc ReturnBlockFunc) []byte {
	return storage.read(pos, returnFunc)
}

type NextBlockFunc func() (uint64, bool)

func (storage SlotStorage) Write(d []byte, nextFreeBlock NextBlockFunc) uint64 {
	var next, slot, first uint64
	var ok bool
	if first, ok = nextFreeBlock(); !ok {
		return EndOfSlots
	}
	slot = first
	for i := 0; i < MaxSlots; i++ {
		if len(d) > MaxPayload {
			if next, ok = nextFreeBlock(); !ok {
				next = EndOfSlots
			}
		} else {
			next = EndOfSlots
		}
		d = storage.At(slot).Write(next, d)
		if next == EndOfSlots {
			return first
		}
		slot = next
	}
	return first
}

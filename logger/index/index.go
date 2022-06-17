package index

import (
	"encoding/binary"
	"time"
)

const (
	counterSize      = 8
	timeSize         = 8
	positionByteSize = 8

	beginByteBegin = 0
	beginByteEnd   = beginByteBegin + positionByteSize
	timeBegin      = beginByteEnd
	timeEnd        = timeBegin + timeSize
	counterBegin   = timeEnd
	counterEnd     = counterBegin + counterSize

	IndexSize = counterEnd

	HeaderSize  = counterSize
	HeaderBegin = 0
	HeaderEnd   = HeaderBegin + HeaderSize
)

var zero = IndexEntry{}

var decodeFunc = binary.BigEndian.Uint64
var encodeFunc = binary.BigEndian.PutUint64

type IntegerType interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64
}

type UnixTimestamp uint64

func (ts UnixTimestamp) Time() time.Time {
	return time.Unix(0, int64(ts))
}

type IndexEntry [IndexSize]byte

func (entry *IndexEntry) Set(begin, counter uint64) {
	encodeFunc(entry[beginByteBegin:beginByteEnd], begin)
	encodeFunc(entry[counterBegin:counterEnd], counter)
	encodeFunc(entry[timeBegin:timeEnd], uint64(time.Now().UnixNano()))
}

func (entry *IndexEntry) Wipe() {
	copy(entry[:], zero[:])
	_ = entry
}

func (entry IndexEntry) BeginByte() uint64 {
	return decodeFunc(entry[beginByteBegin:beginByteEnd])
}

func (entry IndexEntry) UnixTime() UnixTimestamp {
	return UnixTimestamp(decodeFunc(entry[timeBegin:timeEnd]))
}

func (entry IndexEntry) Counter() uint64 {
	return decodeFunc(entry[counterBegin:counterEnd])
}

type IndexField []byte

func Size[T IntegerType](size T) int {
	return int(HeaderSize + size*IndexSize)
}

func New[T IntegerType](size T) IndexField {
	d := IndexField(make([]byte, Size(size)))
	d.setHeader(0)
	return d
}

func (field IndexField) Len() uint64 {
	return uint64((len(field) - HeaderSize) / IndexSize)
}

func (field IndexField) setHeader(i uint64) {
	encodeFunc(field[HeaderBegin:HeaderEnd], i)
}

func (field IndexField) getHeader() uint64 {
	return decodeFunc(field[HeaderBegin:HeaderEnd])
}

func (field IndexField) at(pos uint64) *IndexEntry {
	pos = pos % (field.Len())
	return (*IndexEntry)(field[HeaderEnd+pos*IndexSize : HeaderEnd+(1+pos)*IndexSize])
}

func (field IndexField) At(pos uint64) IndexEntry {
	return *(field.at(pos))
}

func (field IndexField) Wipe(pos uint64) {
	field.at(pos).Wipe()
}

func (field IndexField) Set(begin, counter uint64) {
	field.at(counter).Set(begin, counter)
}

func (field IndexField) Append(begin uint64) (oldBegin uint64, oldTime UnixTimestamp, overwritten bool) {
	counter := field.MaxCounter() + 1
	old := field.At(counter)
	oldBegin = old.BeginByte()
	oldTime = old.UnixTime()
	field.Set(begin, counter)
	field.setHeader(counter)
	if oldTime > 0 {
		return oldBegin, oldTime, true
	}
	return 0, 0, false
}

func (field IndexField) MaxCounter() uint64 {
	var prev uint64
	if c := field.getHeader(); c != 0 {
		return c
	}
	last := len(field) / IndexSize
	for i := uint64(0); i < uint64(last); i++ {
		c := field.at(i).Counter()
		if c < prev {
			return prev
		}
		prev = c
	}
	return prev
}

func (field IndexField) MinCounter() uint64 {
	r := field.MaxCounter()
	l := field.Len()
	if r >= l {
		return r - l + 1
	}
	return 0
}

func (field IndexField) TimeSpan() (UnixTimestamp, UnixTimestamp) {
	return field.At(field.MinCounter()).UnixTime(), field.At(field.MaxCounter()).UnixTime()
}

func max[T IntegerType](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func (field IndexField) First(after uint64) (pos uint64, found bool) {
	for i := max(field.MinCounter(), after); i <= field.MaxCounter(); i++ {
		d := field.At(i)
		if d != zero {
			return i, true
		}
	}
	return 0, false
}

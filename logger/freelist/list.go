// Package freelist implements a fixed size list used for tracking element status (free, drawn).
package freelist

import (
	"math/rand"
	"sync"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Freelist implements a list of element stati.
type Freelist struct {
	e     []uint64
	l     int
	mutex *sync.Mutex
}

//var randFunc = func(m uint64) uint64 { return rand.Uint64() % m }
var nextFunc = func(m uint64) uint64 { return 0 }

// New creates a new Freelist with <elements> number of entries.
func New(elements int) *Freelist {
	r := &Freelist{
		e:     make([]uint64, elements),
		l:     elements - 1,
		mutex: new(sync.Mutex),
	}
	for i := uint64(0); i < uint64(len(r.e)); i++ {
		r.e[i] = i
	}
	return r
}

// Free returns the number of elements that are "free" in the list.
func (list *Freelist) Free() int {
	r := int(list.l) + 1
	if r >= 0 {
		return r
	}
	return 0
}

// Draw an element from the list. Returns false if no elements are available.
func (list *Freelist) Draw() (uint64, bool) {
	var r, next uint64
	if list.l < 0 {
		return 0, false
	}
	if list.l > 0 {
		next = nextFunc(uint64(list.l))
	}
	r = list.e[next]
	list.e[next] = list.e[list.l]
	list.l--
	return r, true
}

// Return an element to the list.
func (list *Freelist) Return(e uint64) {
	list.l++
	list.e[list.l] = e
}

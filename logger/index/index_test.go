package index

import (
	"fmt"
	"testing"
	"time"

	_ "github.com/davecgh/go-spew/spew"
)

func abs(d time.Duration) time.Duration {
	if d < 0 {
		return d * -1
	}
	return d
}

func TestIndex(t *testing.T) {
	size := 10
	f := New(size)
	if len(f) != size*IndexSize+HeaderEnd {
		t.Fatal("Wrong Field Size")
	}
	if f.Len() != uint64(size) {
		t.Fatal("Wrong Field Length")
	}
	if f.MaxCounter() != 0 {
		t.Errorf("0 MaxCounter, %d", f.MaxCounter())
	}
	for i := uint64(0); i < uint64(size); i++ {
		f.Set(100+i, i)
	}
	for i := uint64(0); i < uint64(size); i++ {
		if f.at(i).BeginByte() != 100+i {
			t.Errorf("%d BeginByte", i)
		}
		if f.at(i).Counter() != i {
			t.Errorf("%d Counter", i)
		}
		if abs(time.Since(f.at(i).UnixTime().Time())) > time.Second {
			t.Errorf("%d Time", i)
		}
	}
	if f.MaxCounter() != uint64(size-1) {
		t.Errorf("1 MaxCounter, %d", f.MaxCounter())
	}
	for i := uint64(0); i < uint64(size*2); i++ {
		time.Sleep(time.Millisecond * 100)
		f.Set(100+i, i)
	}
	if f.MaxCounter() != uint64(2*size-1) {
		t.Errorf("2 MaxCounter, %d", f.MaxCounter())
	}
	for i := uint64(0); i < uint64(size+3); i++ {
		time.Sleep(time.Millisecond * 300)
		f.Set(100+i, i)
	}
	first, last := f.TimeSpan()
	if last.Time().Before(first.Time()) || last.Time().Equal(first.Time()) {
		t.Error("TimeSpam")
	}
}

func TestIndexSize(t *testing.T) {
	size := 10
	f := New(size)
	for i := uint64(0); i < 103; i++ {
		f.Append(i)
		if f.MaxCounter() > f.Len() {
			if f.MinCounter()+f.Len()-1 != f.MaxCounter() {
				t.Error("Out of bounds 1")
			}
		} else {
			if (f.MaxCounter() - f.MinCounter()) >= f.Len() {
				t.Error("Out of bounds 2")
			}
		}
	}
	for i := f.MinCounter(); i <= f.MaxCounter(); i++ {
		fmt.Println(f.At(i).BeginByte())
	}
}

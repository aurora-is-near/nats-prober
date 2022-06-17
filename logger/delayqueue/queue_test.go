package delayqueue

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func PrintFunc(d []byte) {
	fmt.Printf("Send: %s\n", string(d))
}

func DumpFunc(d []byte) {
	fmt.Printf("Dump: %s\n", string(d))
}

func TestDelayQueue(t *testing.T) {
	dir, _ := os.MkdirTemp("/tmp", "delayqueue.")
	queue, err := Init(dir, 5, PrintFunc, DumpFunc)
	if err != nil {
		t.Fatalf("Init: %s", err)
	}
	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("Message: %d", i))
		fmt.Printf("Queue: %s\n", string(msg))
		queue.AddMsg(msg)
		if i == 30 {
			queue.Dump()
		}
		time.Sleep(time.Millisecond * 100)
	}
	time.Sleep(time.Second * 10)
}

package prober

import (
	"hash/maphash"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/aurora-is-near/nats-rpc-prober/transport"
	"github.com/nats-io/nats.go"
)

type Prober struct {
	Nats                     *transport.NatsConnection
	RequestSubjects          []string
	ResponseSubjects         []string
	RequestTimeoutSeconds    uint
	WorkersCount             uint
	WorkerMaxPendingRequests uint

	handlersWg sync.WaitGroup
	workers    []*Worker
}

func (prober *Prober) Run() error {
	if err := prober.Nats.Connect(); err != nil {
		return err
	}
	defer prober.Nats.Drain()

	log.Printf("Starting workers...")
	prober.workers = make([]*Worker, prober.WorkersCount)
	for i := 0; i < int(prober.WorkersCount); i++ {
		prober.workers[i] = StartWorker(prober)
		defer prober.workers[i].Stop()
	}
	defer log.Printf("Stopping workers...")

	defer prober.handlersWg.Wait()
	defer log.Printf("Waiting for handlers to finish...")

	log.Printf("Subscribing to requests...")
	for _, subject := range prober.RequestSubjects {
		sub, err := prober.Nats.GetConnection().Subscribe(subject, prober.handleRequest)
		if err != nil {
			return err
		}
		defer sub.Unsubscribe()
	}
	defer log.Printf("Unsubscribing from requests...")

	log.Printf("Subscribing to responses...")
	for _, subject := range prober.ResponseSubjects {
		sub, err := prober.Nats.GetConnection().Subscribe(subject, prober.handleResponse)
		if err != nil {
			return err
		}
		defer sub.Unsubscribe()
	}
	defer log.Printf("Unsubscribing from responses...")

	interrupt := make(chan os.Signal, 10)
	signal.Notify(interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT)

	log.Printf("Serving...")
	select {
	case err := <-prober.Nats.GetClosed():
		return err
	case <-interrupt:
	}

	return nil
}

func (prober *Prober) handleRequest(request *nats.Msg) {
	prober.handlersWg.Add(1)
	defer prober.handlersWg.Done()
	message := NewMessage(request)
	prober.getWorker(request.Reply).AddRequest(message)
}

func (prober *Prober) handleResponse(response *nats.Msg) {
	prober.handlersWg.Add(1)
	defer prober.handlersWg.Done()
	message := NewMessage(response)
	prober.getWorker(response.Subject).AddResponse(message)
}

func (prober *Prober) getWorker(replySubject string) *Worker {
	var hash maphash.Hash
	hash.WriteString(replySubject)
	index := int(hash.Sum64() % uint64(prober.WorkersCount))
	return prober.workers[index]
}

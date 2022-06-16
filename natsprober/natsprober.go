package natsprober

import (
	"hash/maphash"
	"log"
	"sync"

	"github.com/nats-io/nats.go"
)

type NatsProber struct {
	RequestSubjects          []string
	ResponseSubjects         []string
	RequestTimeoutSeconds    uint
	WorkersCount             uint
	WorkerMaxPendingRequests uint

	successfulResponseHandler func(request *NatsMessage, response *NatsMessage)
	timeoutedRequestHandler   func(request *NatsMessage)
	unknownResponseHandler    func(response *NatsMessage)
	droppedRequestHandler     func(request *NatsMessage)

	workers       []*worker
	subscriptions []*nats.Subscription
	handlersWg    sync.WaitGroup
}

func (prober *NatsProber) SetSuccessfulResponseHandler(handler func(request *NatsMessage, response *NatsMessage)) {
	prober.successfulResponseHandler = handler
}

func (prober *NatsProber) SetTimeoutedRequestHandler(handler func(request *NatsMessage)) {
	prober.timeoutedRequestHandler = handler
}

func (prober *NatsProber) SetUnknownResponseHandler(handler func(response *NatsMessage)) {
	prober.unknownResponseHandler = handler
}

func (prober *NatsProber) SetDroppedRequestHandler(handler func(request *NatsMessage)) {
	prober.droppedRequestHandler = handler
}

func (prober *NatsProber) Start(nc *nats.Conn) error {
	log.Printf("NatsProber: starting workers...")
	for i := 0; i < int(prober.WorkersCount); i++ {
		prober.workers = append(prober.workers, startWorker(prober))
	}

	log.Printf("NatsProber: subscribing to requests...")
	for _, subject := range prober.RequestSubjects {
		sub, err := nc.Subscribe(subject, prober.handleRequest)
		if err != nil {
			prober.Stop()
			return err
		}
		prober.subscriptions = append(prober.subscriptions, sub)
	}

	log.Printf("NatsProber: subscribing to responses...")
	for _, subject := range prober.ResponseSubjects {
		sub, err := nc.Subscribe(subject, prober.handleResponse)
		if err != nil {
			prober.Stop()
			return err
		}
		prober.subscriptions = append(prober.subscriptions, sub)
	}

	return nil
}

func (prober *NatsProber) Stop() error {
	log.Printf("NatsProber: unsubscribing...")
	var unsubErr error
	for _, sub := range prober.subscriptions {
		if err := sub.Unsubscribe(); err != nil {
			unsubErr = err
		}
	}

	if unsubErr == nil {
		log.Printf("NatsProber: waiting for handlers to finish...")
		prober.handlersWg.Wait()
	}

	log.Printf("NatsProber: stopping workers...")
	for _, w := range prober.workers {
		w.stop()
	}

	return unsubErr
}

func (prober *NatsProber) handleRequest(request *nats.Msg) {
	prober.handlersWg.Add(1)
	defer prober.handlersWg.Done()
	message := newNatsMessage(request)
	prober.getWorker(request.Reply).addRequest(message)
}

func (prober *NatsProber) handleResponse(response *nats.Msg) {
	prober.handlersWg.Add(1)
	defer prober.handlersWg.Done()
	message := newNatsMessage(response)
	prober.getWorker(response.Subject).addResponse(message)
}

func (prober *NatsProber) getWorker(replySubject string) *worker {
	var hash maphash.Hash
	hash.WriteString(replySubject)
	index := int(hash.Sum64() % uint64(prober.WorkersCount))
	return prober.workers[index]
}

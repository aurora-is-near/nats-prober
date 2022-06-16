package prober

import (
	"sync"
	"time"

	"github.com/aurora-is-near/nats-rpc-prober/linkedmap"
)

type Worker struct {
	prober *Prober

	requests  chan *NatsMessage
	responses chan *NatsMessage
	stop      chan bool
	wg        sync.WaitGroup

	requestsQueue *linkedmap.LinkedMap[string, *NatsMessage]
}

func StartWorker(prober *Prober) *Worker {
	worker := &Worker{
		prober:        prober,
		requests:      make(chan *NatsMessage, 100),
		responses:     make(chan *NatsMessage, 100),
		stop:          make(chan bool),
		requestsQueue: linkedmap.New[string, *NatsMessage](),
	}

	worker.wg.Add(1)
	go worker.run()
	return worker
}

func (worker *Worker) Stop() {
	worker.stop <- true
	worker.wg.Wait()
}

func (worker *Worker) AddRequest(request *NatsMessage) {
	worker.requests <- request
}

func (worker *Worker) AddResponse(response *NatsMessage) {
	worker.responses <- response
}

func (worker *Worker) run() {
	defer worker.wg.Done()

	timeoutsCheckTicker := time.NewTicker(time.Second / 10)
	defer timeoutsCheckTicker.Stop()

	for {
		// Prioritized stop-check
		select {
		case <-worker.stop:
			return
		default:
		}

		// Prioritized timeouts-check
		select {
		case <-timeoutsCheckTicker.C:
			worker.checkTimeouts()
		default:
		}

		select {
		case request := <-worker.requests:
			worker.handleRequest(request)
		case response := <-worker.responses:
			worker.handleResponse(response)
		case <-timeoutsCheckTicker.C:
			worker.checkTimeouts()
		case <-worker.stop:
			return
		}
	}
}

func (worker *Worker) checkTimeouts() {
	for {
		oldest, ok := worker.requestsQueue.GetFirst()
		if !ok {
			return
		}
		if time.Since(oldest.ReceivedAt) < time.Duration(worker.prober.RequestTimeoutSeconds)*time.Second {
			return
		}
		worker.requestsQueue.PopFirst()

		worker.recordTimeoutedRequest(oldest)
	}
}

func (worker *Worker) handleRequest(request *NatsMessage) {
	if worker.requestsQueue.Len() == int(worker.prober.WorkerMaxQueueSize) {
		droppedRequest, _ := worker.requestsQueue.PopFirst()
		worker.recordDroppedRequest(droppedRequest)
	}
	worker.requestsQueue.PushLast(request.Msg.Reply, request)
}

func (worker *Worker) handleResponse(response *NatsMessage) {
	request, ok := worker.requestsQueue.Get(response.Msg.Subject)
	if !ok {
		worker.recordUnknownResponse(response)
		return
	}
	worker.recordSuccessfulResponse(request, response)
}

func (worker *Worker) recordTimeoutedRequest(request *NatsMessage) {
	// TODO
}

func (worker *Worker) recordDroppedRequest(request *NatsMessage) {
	// TODO
}

func (worker *Worker) recordUnknownResponse(response *NatsMessage) {
	// TODO
}

func (worker *Worker) recordSuccessfulResponse(request *NatsMessage, response *NatsMessage) {
	// TODO
}

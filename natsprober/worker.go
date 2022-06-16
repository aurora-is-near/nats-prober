package natsprober

import (
	"sync"
	"time"

	"github.com/aurora-is-near/nats-prober/linkedmap"
)

type worker struct {
	prober *NatsProber

	requests  chan *NatsMessage
	responses chan *NatsMessage
	stopChan  chan bool
	wg        sync.WaitGroup

	pendingRequests *linkedmap.LinkedMap[string, *NatsMessage]
}

func startWorker(prober *NatsProber) *worker {
	w := &worker{
		prober:          prober,
		requests:        make(chan *NatsMessage, 100),
		responses:       make(chan *NatsMessage, 100),
		stopChan:        make(chan bool),
		pendingRequests: linkedmap.New[string, *NatsMessage](),
	}

	w.wg.Add(1)
	go w.run()
	return w
}

func (w *worker) stop() {
	w.stopChan <- true
	w.wg.Wait()
}

func (w *worker) addRequest(request *NatsMessage) {
	w.requests <- request
}

func (w *worker) addResponse(response *NatsMessage) {
	w.responses <- response
}

func (w *worker) run() {
	defer w.wg.Done()

	timeoutsCheckTicker := time.NewTicker(time.Second / 10)
	defer timeoutsCheckTicker.Stop()

	for {
		// Prioritized stop-check
		select {
		case <-w.stopChan:
			return
		default:
		}

		// Prioritized timeouts-check
		select {
		case <-timeoutsCheckTicker.C:
			w.checkTimeouts()
		default:
		}

		select {
		case request := <-w.requests:
			w.handleRequest(request)
		case response := <-w.responses:
			w.handleResponse(response)
		case <-timeoutsCheckTicker.C:
			w.checkTimeouts()
		case <-w.stopChan:
			return
		}
	}
}

func (w *worker) checkTimeouts() {
	for {
		oldest, ok := w.pendingRequests.GetFirst()
		if !ok {
			return
		}
		if time.Since(oldest.ReceivedAt) < time.Duration(w.prober.RequestTimeoutSeconds)*time.Second {
			return
		}
		w.pendingRequests.PopFirst()

		if w.prober.timeoutedRequestHandler != nil {
			w.prober.timeoutedRequestHandler(oldest)
		}
	}
}

func (w *worker) handleRequest(request *NatsMessage) {
	if w.pendingRequests.Len() == int(w.prober.WorkerMaxPendingRequests) {
		droppedRequest, _ := w.pendingRequests.PopFirst()
		if w.prober.droppedRequestHandler != nil {
			w.prober.droppedRequestHandler(droppedRequest)
		}
	}
	w.pendingRequests.PushLast(request.Msg.Reply, request)
}

func (w *worker) handleResponse(response *NatsMessage) {
	request, ok := w.pendingRequests.Get(response.Msg.Subject)
	if !ok {
		if w.prober.unknownResponseHandler != nil {
			w.prober.unknownResponseHandler(response)
		}
		return
	}
	if w.prober.successfulResponseHandler != nil {
		w.prober.successfulResponseHandler(request, response)
	}
}

package logger

import (
	"log"
	"sync"
	"time"

	"github.com/aurora-is-near/nats-prober/logger/batchcompress"
	"github.com/aurora-is-near/nats-prober/logger/delayqueue"
	"github.com/nats-io/nats.go"
)

type Logger struct {
	RealtimeSubject    string
	DelayedSubject     string
	DumpSubject        string
	DumpTriggerSubject string

	EnableDelayQueue bool
	DelayMinutes     uint
	DelayQueueSize   uint
	DelayQueueFile   string

	EnableBatching    bool
	MaxBatchSizeBytes uint
	MaxBatchAgeMs     uint

	natsConn *nats.Conn

	delayQueue              *delayqueue.DelayQueue
	batcher                 *batchcompress.Batcher
	dumpTriggerSubscription *nats.Subscription

	delayQueueHandlerWg  sync.WaitGroup
	batcherHandlerWg     sync.WaitGroup
	dumpTriggerHandlerWg sync.WaitGroup
}

func (logger *Logger) Start(natsConn *nats.Conn) error {
	logger.natsConn = natsConn

	if logger.EnableDelayQueue {

		log.Printf("Logger: starting delay-queue...")
		var err error
		logger.delayQueue, err = delayqueue.NewWriteable(
			time.Minute*time.Duration(logger.DelayMinutes),
			int(logger.DelayQueueSize),
			logger.DelayQueueFile,
			logger.handleDelayedData,
			logger.handleDumpedData,
		)
		if err != nil {
			return err
		}

		log.Printf("Logger: subscribing to dump-trigger...")
		logger.dumpTriggerSubscription, err = natsConn.Subscribe(logger.DumpTriggerSubject, logger.handleDumpTrigger)
		if err != nil {
			logger.Stop()
			return err
		}
	}

	if logger.EnableBatching {
		log.Printf("Logger: starting batcher...")

		logger.batcher = batchcompress.NewBatcher(
			int(logger.MaxBatchSizeBytes),
			time.Millisecond*time.Duration(logger.MaxBatchAgeMs),
			logger.handleBatch,
		)
	}

	return nil
}

func (logger *Logger) Stop() {
	if logger.batcher != nil {
		log.Printf("Logger: stopping batcher...")
		logger.batcher.Close()
		logger.batcherHandlerWg.Wait()
	}

	if logger.delayQueue != nil {
		if logger.dumpTriggerSubscription != nil {
			log.Printf("Logger: unsubscribing from dump-trigger...")
			logger.dumpTriggerSubscription.Unsubscribe()
			logger.dumpTriggerHandlerWg.Wait()
		}

		log.Printf("Logger: stopping delay-queue...")
		logger.delayQueue.Stop()
		logger.delayQueueHandlerWg.Wait()
	}
}

func (logger *Logger) AddLogLine(data []byte, subjectSuffix string) {
	if logger.batcher != nil {
		logger.batcher.Add(data)
	} else {
		logger.handleLogLine(data, subjectSuffix)
	}
}

func (logger *Logger) handleDelayedData(data []byte) {
	logger.delayQueueHandlerWg.Add(1)
	defer logger.delayQueueHandlerWg.Done()

	if err := logger.natsConn.Publish(logger.DelayedSubject, data); err != nil {
		log.Printf("Logger: can't publish delayed data: %v", err)
	}
}

func (logger *Logger) handleDumpedData(data []byte) {
	logger.delayQueueHandlerWg.Add(1)
	defer logger.delayQueueHandlerWg.Done()

	if err := logger.natsConn.Publish(logger.DumpSubject, data); err != nil {
		log.Printf("Logger: can't publish dumped data: %v", err)
	}
}

func (logger *Logger) handleDumpTrigger(msg *nats.Msg) {
	logger.dumpTriggerHandlerWg.Add(1)
	defer logger.dumpTriggerHandlerWg.Done()

	logger.delayQueue.Dump()
}

func (logger *Logger) handleBatch(data []byte) {
	logger.batcherHandlerWg.Add(1)
	defer logger.batcherHandlerWg.Done()

	logger.handleLogLine(data, "")
}

func (logger *Logger) handleLogLine(data []byte, subjectSuffix string) {
	if logger.delayQueue != nil {
		logger.delayQueue.AddMsg(data)
	}

	if err := logger.natsConn.Publish(logger.RealtimeSubject+subjectSuffix, data); err != nil {
		log.Printf("Logger: can't publish realtime data: %v", err)
	}
}

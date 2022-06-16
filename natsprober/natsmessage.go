package natsprober

import (
	"time"

	"github.com/nats-io/nats.go"
)

type NatsMessage struct {
	Msg        *nats.Msg
	ReceivedAt time.Time
}

func newNatsMessage(msg *nats.Msg) *NatsMessage {
	return &NatsMessage{
		Msg:        msg,
		ReceivedAt: time.Now(),
	}
}

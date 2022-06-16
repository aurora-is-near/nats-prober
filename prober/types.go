package prober

import (
	"time"

	"github.com/nats-io/nats.go"
)

type NatsMessage struct {
	Msg        *nats.Msg
	ReceivedAt time.Time
}

func NewMessage(msg *nats.Msg) *NatsMessage {
	return &NatsMessage{
		Msg:        msg,
		ReceivedAt: time.Now(),
	}
}

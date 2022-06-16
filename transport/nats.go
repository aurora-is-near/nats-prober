package transport

import (
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type NatsConnection struct {
	Endpoints           []string
	Creds               string
	PingIntervalMs      uint
	TimeoutMs           uint
	MaxPingsOutstanding int
	LogTag              string

	connection *nats.Conn
	closed     chan error
	errorChan  chan<- error
}

func (conn *NatsConnection) SetErrorChan(errorChan chan<- error) {
	conn.errorChan = errorChan
}

func (conn *NatsConnection) Connect() error {
	if conn.PingIntervalMs == 0 {
		conn.PingIntervalMs = 2000
	}
	if conn.TimeoutMs == 0 {
		conn.TimeoutMs = 4000
	}
	if conn.MaxPingsOutstanding == 0 {
		conn.MaxPingsOutstanding = 2
	}
	if len(conn.LogTag) == 0 {
		conn.LogTag = "general"
	}

	log.Printf("Connecting to NATS [%v]...", conn.LogTag)

	options := []nats.Option{
		nats.Name("nats2relayer"),
		nats.ReconnectWait(time.Second / 5),
		nats.PingInterval(time.Duration(conn.PingIntervalMs) * time.Millisecond),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.MaxPingsOutstanding(conn.MaxPingsOutstanding),
		nats.Timeout(time.Duration(conn.TimeoutMs) * time.Millisecond),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			log.Printf("NATS [%v] error: %v", conn.LogTag, err)
			if conn.errorChan != nil {
				conn.errorChan <- err
			}
		}),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				log.Printf("NATS [%v] disconnected due to: %v, will try reconnecting", conn.LogTag, err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("NATS [%v] reconnected [%v]", conn.LogTag, nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			err := nc.LastError()
			if err != nil {
				log.Printf("NATS [%v] connection closed: %v", conn.LogTag, err)
			}
			conn.closed <- err
		}),
	}
	if len(conn.Creds) > 0 {
		options = append(options, nats.UserCredentials(conn.Creds))
	}

	conn.closed = make(chan error, 1)
	var err error
	conn.connection, err = nats.Connect(strings.Join(conn.Endpoints, ", "), options...)
	return err
}

func (conn *NatsConnection) CheckHealth() error {
	if err := conn.connection.Flush(); err != nil {
		return err
	}
	return conn.connection.LastError()
}

func (conn *NatsConnection) Drain() error {
	log.Printf("Draining NATS [%v] connection...", conn.LogTag)
	if err := conn.connection.Drain(); err != nil {
		log.Printf("Error when draining NATS [%v] connection: %v", conn.LogTag, err)
		return err
	}
	return nil
}

func (conn *NatsConnection) GetConnection() *nats.Conn {
	return conn.connection
}

func (conn *NatsConnection) GetClosed() <-chan error {
	return conn.closed
}

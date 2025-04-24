package bus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/streadway/amqp"
)

type bus struct {
	conn    *amqp.Connection
	config  Config
	closeCh chan *amqp.Error
}

func (b *bus) reconnect() error {
	for {
		chErr := <-b.closeCh
		maxRetriesReached := false
		if chErr != nil {

			for i := 0; i < b.config.MaxConnectionRetries; i++ {
				if i > 0 {
					duration := time.Duration(i*10) * time.Second
					slog.Debug(fmt.Sprintf("sleeping for %v seconds", duration.Seconds()))
					time.Sleep(duration)
				}

				slog.Info(fmt.Sprintf("retrying dial up, attempt %d", i+1))

				conn, err := amqp.Dial(b.config.ServerConfig.GetAMQPUrl())
				if err != nil {
					if i+1 == b.config.MaxConnectionRetries {
						maxRetriesReached = true
						break
					}
					continue
				}

				slog.Info("connection reestablished")
				b.conn = conn
				b.closeCh = make(chan *amqp.Error)
				b.conn.NotifyClose(b.closeCh)
				break

			}
			if maxRetriesReached {
				return errors.New("connection retries exceeded")
			}
		}
	}
	return nil
}

// Bus - interface for interacting with the rabbitmq message bus
type Bus interface {
	Close() error

	Declare(sub Subscription) error
	ListQueues() ([]string, error)
	QueueDetails(queue string) (QueueDetails, error)
	PurgeQueue(queue string) error
	Listen(subscriptions []Subscription) error
	ListenRaw(queue string, handler func(body []byte) error) error
	ListenRawDel(queue string, handler func(del amqp.Delivery) error) error
	Send(ctx context.Context, msg Message, headers map[string]interface{}) error
	SendRaw(ctx context.Context, exchange string, route string, body []byte, headers map[string]interface{}) error
	reconnect() error
}

// NewBus - create a new bus
func NewBus(config Config) (Bus, error) {
	slog.Debug(fmt.Sprintf("connecting to rabbitmq server at: %s", config.ServerConfig.GetAMQPUrl()))
	conn, err := amqp.Dial(config.ServerConfig.GetAMQPUrl())
	if err != nil {
		return nil, err
	}

	bus := bus{conn: conn, config: config}
	bus.closeCh = make(chan *amqp.Error)

	bus.conn.NotifyClose(bus.closeCh)
	go func() {
		err := bus.reconnect()
		if err != nil {
			log.Fatalf("bus error: %v", err)
		}
	}()

	return &bus, nil
}

func (b bus) Close() error {
	return b.conn.Close()
}

type TestBus struct {
}

func (t TestBus) Close() error {
	return nil
}
func (t TestBus) Declare(sub Subscription) error {
	return nil
}
func (t TestBus) ListQueues() ([]string, error) {
	return nil, nil
}
func (t TestBus) QueueDetails(queue string) (QueueDetails, error) {
	return QueueDetails{}, nil
}
func (t TestBus) PurgeQueue(queue string) error {
	return nil
}
func (t TestBus) Listen(subscriptions []Subscription) error {
	return nil
}
func (t TestBus) ListenRaw(queue string, handler func(body []byte) error) error {
	return nil
}
func (t TestBus) ListenRawDel(queue string, handler func(del amqp.Delivery) error) error {
	return nil
}
func (t TestBus) Send(ctx context.Context, msg Message, headers map[string]interface{}) error {
	return nil
}
func (t TestBus) SendRaw(ctx context.Context, exchange string, route string, body []byte, headers map[string]interface{}) error {
	return nil
}
func (t TestBus) reconnect() error {
	return nil
}

type ErrorBus struct {
}

func (t ErrorBus) Close() error {
	return errors.New("test fail")
}
func (t ErrorBus) Declare(sub Subscription) error {
	return errors.New("test fail")
}
func (t ErrorBus) ListQueues() ([]string, error) {
	return nil, errors.New("test fail")
}
func (t ErrorBus) QueueDetails(queue string) (QueueDetails, error) {
	return QueueDetails{}, errors.New("test fail")
}
func (t ErrorBus) PurgeQueue(queue string) error {
	return errors.New("test fail")
}
func (t ErrorBus) Listen(subscriptions []Subscription) error {
	return errors.New("test fail")
}
func (t ErrorBus) ListenRaw(queue string, handler func(body []byte) error) error {
	return errors.New("test fail")
}
func (t ErrorBus) ListenRawDel(queue string, handler func(del amqp.Delivery) error) error {
	return errors.New("test fail")
}
func (t ErrorBus) Send(ctx context.Context, msg Message, headers map[string]interface{}) error {
	return errors.New("test fail")
}
func (t ErrorBus) SendRaw(ctx context.Context, exchange string, route string, body []byte, headers map[string]interface{}) error {
	return errors.New("test fail")
}
func (t ErrorBus) reconnect() error {
	return errors.New("test fail")
}

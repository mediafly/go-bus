package bus

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

// Subscription - represents a binding between an exchange/queue and a handler func
type Subscription struct {
	Redundancy int
	Queue      string
	Route      string
	Exchange   string
	Handler    func(body []byte, headers amqp.Table) error
}

// DLQName - returns the name of the associated DLQ
func (s Subscription) DLQName() string {
	return fmt.Sprintf("%s.DLQ", s.Queue)
}

// Declare - declare queues based on the prvided binding
func (b bus) Declare(sub Subscription) error {
	ch, err := b.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	durable := b.config.Durable

	log.Printf("declaring queue: %s", sub.Queue)
	_, err = ch.QueueDeclare(sub.Queue, durable, false, false, false, nil)
	if err != nil {
		return err
	}

	log.Printf("declaring queue: %s", sub.DLQName())
	_, err = ch.QueueDeclare(sub.DLQName(), durable, false, false, false, nil)
	if err != nil {
		return err
	}

	log.Printf("declaring exchange: %s (type: %s)", sub.Exchange, b.config.ExchangeType)
	err = ch.ExchangeDeclare(sub.Exchange, b.config.ExchangeType, durable, false, false, false, nil)
	if err != nil {
		return err
	}

	log.Printf("binding queue %s to exchange/route: %s/%s", sub.Queue, sub.Exchange, sub.Route)
	err = ch.QueueBind(sub.Queue, sub.Route, sub.Exchange, false, nil)
	if err != nil {
		return err
	}

	return nil
}

package bus

import (
	"github.com/streadway/amqp"
)

type consumeError struct {
	err error
	sub Subscription
}

// DeadLetter - represents a message whose handler threw an error during processing
type DeadLetter struct {
	Message string
	Error   string
}

// Listen - listen for messages on each queue binding in a seperate goroutine
func (b bus) Listen(subscriptions []Subscription) error {
	consumeErrors := make(chan consumeError, len(subscriptions))

	for _, sub := range subscriptions {
		err := b.Declare(sub)
		if err != nil {
			return err
		}

		if sub.Redundancy == 0 {
			go b.consume(sub, consumeErrors)
		}

		for i := 0; i < sub.Redundancy; i++ {
			go b.consume(sub, consumeErrors)
		}

	}

	retries := 0
	for ce := range consumeErrors {
		b.log.Printf("error handling messages on queue %s: %v", ce.sub.Queue, ce.err)
		if retries < b.config.MaxConnectionRetries {
			retries++
			b.log.Printf("attempting to restart consumer for queue: %s (retry: %d; max retries: %d)", ce.sub.Queue, retries, b.config.MaxConnectionRetries)

			go b.consume(ce.sub, consumeErrors)
		} else {
			b.log.Printf("max retries exceeded")
			return ce.err
		}
	}

	return nil
}

// Consume - Listen for messages on the sepficied queue binding
func (b bus) consume(sub Subscription, abort chan consumeError) {
	b.log.Printf("listening for messages on queue: %s", sub.Queue)

	ch, err := b.conn.Channel()
	if err != nil {
		abort <- consumeError{err: err, sub: sub}
		return
	}
	defer ch.Close()

	go func() {
		x := <-ch.NotifyClose(make(chan *amqp.Error))
		if x != nil {
			err = Wrap(x, "channel closed unexpectedly")
			abort <- consumeError{err: err, sub: sub}
		}
	}()

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		abort <- consumeError{err: err, sub: sub}
		return
	}
	messages, err := ch.Consume(sub.Queue, "", false, false, false, false, nil)
	if err != nil {
		abort <- consumeError{err: err, sub: sub}
		return
	}

	for del := range messages {
		b.log.Printf("message with length %d bytes received on queue: %s headers %v", len(del.Body), sub.Queue, del.Headers)
		defer func() {
			if r := recover(); r != nil {
				b.log.Printf("PANIC occured handling message on queue %s: %v", sub.Queue, r)
				err = Newf("PANIC: %v", r)

				err = del.Nack(false, false)
				if err != nil {
					abort <- consumeError{err: err, sub: sub}
					return
				}
				pub := b.deliveryToPublishing(del, r.(error)) // Assert r as error type

				err = ch.Publish("", sub.DLQName(), false, false, pub)
				if err != nil {
					abort <- consumeError{err: err, sub: sub}
					return
				}
			}
		}()

		msgerr := sub.Handler(del.Body, del.Headers)
		if msgerr != nil {
			b.log.Printf("error handling message on queue %s: %v", sub.Queue, msgerr)
			err = del.Nack(false, false)
			if err != nil {
				abort <- consumeError{err: err, sub: sub}
				return
			}

			pub := b.deliveryToPublishing(del, msgerr)

			err = ch.Publish("", sub.DLQName(), false, false, pub)
			if err != nil {
				abort <- consumeError{err: err, sub: sub}
				return
			}
		} else {
			//b.log.Printf("message on queue %s handled successfully", sub.Queue)

			err = del.Ack(false)
			if err != nil {
				abort <- consumeError{err: err, sub: sub}
				return
			}

			b.log.Printf("message on queue %s acked.", sub.Queue)
		}
	}
}

func (b bus) deliveryToPublishing(d amqp.Delivery, err error) amqp.Publishing {
	headers := d.Headers
	if headers == nil {
		headers = amqp.Table{}
	}

	headers["mferrormessage"] = err.Error()
	headers["mfroute"] = d.RoutingKey
	headers["mfexchange"] = d.Exchange

	headers["error"] = err.Error()
	headers["route"] = d.RoutingKey
	headers["exchange"] = d.Exchange

	return amqp.Publishing{
		Headers:         headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		Expiration:      d.Expiration,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		UserId:          d.UserId,
		AppId:           d.AppId,
		Body:            d.Body,
	}
}

// ListenRaw - listen for messages using the given handler on the specified queue. Note that messages will not be archived, and neither the queue itself nor a DLQ will be declared.
func (b bus) ListenRaw(queue string, handler func(body []byte) error) error {
	b.log.Printf("listening for messages on queue: %s", queue)

	ch, err := b.conn.Channel()
	if err != nil {
		return Wrap(err, "error establishing channel")
	}
	defer ch.Close()

	messages, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return Wrapf(err, "error consuming queue %s", queue)
	}

	for del := range messages {
		b.log.Printf("message with length %d bytes received on queue: %s", len(del.Body), queue)

		msgerr := handler(del.Body)
		if msgerr != nil {
			b.log.Printf("error handling message on queue %s: %v", queue, msgerr)
			err = del.Nack(false, true)
			if err != nil {
				return Wrapf(err, "error nacking message on queue: %s", queue)
			}

		} else {
			err = del.Ack(false)
			if err != nil {
				return Wrapf(err, "error acking message on queue: %s", queue)
			}
		}
	}

	return nil
}

// ListenRawHeaders - same as ListenRaw, but allows message headers to be passed into the handler
func (b bus) ListenRawDel(queue string, handler func(amqp.Delivery) error) error {
	b.log.Printf("listening for messages on queue: %s", queue)

	ch, err := b.conn.Channel()
	if err != nil {
		return Wrap(err, "error establishing channel")
	}
	defer ch.Close()

	messages, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return Wrapf(err, "error consuming queue %s", queue)
	}

	for del := range messages {
		b.log.Printf("message with length %d bytes received on queue: %s", len(del.Body), queue)

		msgerr := handler(del)
		if msgerr != nil {
			b.log.Printf("error handling message on queue %s: %v", queue, msgerr)
			err = del.Nack(false, true)
			if err != nil {
				return Wrapf(err, "error nacking message on queue: %s", queue)
			}

		} else {
			err = del.Ack(false)
			if err != nil {
				return Wrapf(err, "error acking message on queue: %s", queue)
			}
		}
	}

	return nil
}

package bus

import (
	"context"
	"encoding/json"

	"github.com/streadway/amqp"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

// Send - send a message on the specified queue using the specified rabbitmq bus config
func (b bus) Send(ctx context.Context, msg Message, headers map[string]interface{}) error {
	span, _ := tracer.StartSpanFromContext(ctx, "bus.Send")
	defer span.Finish()

	span.SetTag("exchange", msg.Exchange())
	span.SetTag("route", msg.Route())

	b.log.Printf("sending message to exchange/route: %s/%s", msg.Exchange(), msg.Route())

	ch, err := b.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	body, err := json.Marshal(&msg)
	if err != nil {
		return err
	}

	pub := amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
		Headers:     headers,
	}

	err = ch.Publish(msg.Exchange(), msg.Route(), false, false, pub)
	if err != nil {
		return err
	}
	return nil
}

// SendRaw - send message not wrapped in bus.Message interface
func (b bus) SendRaw(ctx context.Context, exchange string, route string, body []byte, headers map[string]interface{}) error {
	span, _ := tracer.StartSpanFromContext(ctx, "bus.SendRaw")
	defer span.Finish()

	span.SetTag("exchange", exchange)
	span.SetTag("route", route)

	b.log.Printf("sending message to exchange/route: %s/%s", exchange, route)

	ch, err := b.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	pub := amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
		Headers:     headers,
	}

	err = ch.Publish(exchange, route, false, false, pub)
	if err != nil {
		return err
	}
	return nil
}

package middleware

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	baseMiddleware
	exchangeName string
	routingKeys  []string
	queueName    string
}

func NewExchangeMiddleware(conn *amqp.Connection, ch *amqp.Channel, exchangeName string, keys []string) *ExchangeMiddleware {
	ctx, cancel := context.WithCancel(context.Background())
	return &ExchangeMiddleware{
		baseMiddleware: baseMiddleware{
			conn:   conn,
			ch:     ch,
			ctx:    ctx,
			cancel: cancel,
		},
		exchangeName: exchangeName,
		routingKeys:  keys,
		queueName:    "",
	}
}

func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if e.ch == nil {
		e.Close()
		return ErrMessageMiddlewareDisconnected
	}

	// Declarar y bindear la cola solo la primera vez que se consume
	if e.queueName == "" {
		queue, err := e.ch.QueueDeclare(
			"",    // name
			false, // durability
			false, // delete when unused
			true,  // exclusive
			false, // no-wait
			nil,   // arguments
		)

		if err != nil {
			e.Close()
			return ErrMessageMiddlewareMessage
		}
		e.queueName = queue.Name

		for _, key := range e.routingKeys {
			err = e.ch.QueueBind(
				e.queueName,    // queue name
				key,            // routing key
				e.exchangeName, // exchange
				false,
				nil)
			if err != nil {
				e.Close()
				return ErrMessageMiddlewareMessage
			}
		}
	}

	e.consumerTag = fmt.Sprintf("consumer-%s-%d", e.queueName, time.Now().UnixNano())

	err := e.ch.Qos(
		10,    // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		e.Close()
		return ErrMessageMiddlewareMessage
	}

	msgs, err := e.ch.Consume(
		e.queueName,   // queue
		e.consumerTag, // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		e.Close()
		return ErrMessageMiddlewareMessage
	}

	e.wg.Add(1)
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			return nil
		case d, ok := <-msgs:
			if !ok {
				return ErrMessageMiddlewareDisconnected
			}
			callbackFunc(Message{Body: string(d.Body)}, func() { d.Ack(false) }, func() { d.Nack(false, true) })
		}
	}
}

func (e *ExchangeMiddleware) Send(msg Message) error {
	if e.ch == nil {
		e.Close()
		return ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, key := range e.routingKeys {
		err := e.ch.PublishWithContext(ctx,
			e.exchangeName, // exchange
			key,            // routing key
			false,          // mandatory
			false,          // immediate
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(msg.Body),
			})
		if err != nil {
			return ErrMessageMiddlewareMessage
		}
	}
	return nil
}

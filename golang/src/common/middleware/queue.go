package middleware

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	baseMiddleware
	queueName string
}

func NewQueueMiddleware(conn *amqp.Connection, ch *amqp.Channel, queueName string) *QueueMiddleware {
	ctx, cancel := context.WithCancel(context.Background())
	return &QueueMiddleware{
		baseMiddleware: baseMiddleware{
			conn:   conn,
			ch:     ch,
			ctx:    ctx,
			cancel: cancel,
		},
		queueName: queueName,
	}
}

func (e *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if e.ch == nil {
		e.Close()
		return ErrMessageMiddlewareDisconnected
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

func (e *QueueMiddleware) Send(msg Message) error {
	if e.ch == nil {
		e.Close()
		return ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := e.ch.PublishWithContext(ctx,
		"",          // exchange
		e.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(msg.Body),
		})
	if err != nil {
		return ErrMessageMiddlewareMessage
	}
	return nil
}

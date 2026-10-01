package middleware

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type baseMiddleware struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	consumerTag string
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

func (b *baseMiddleware) Close() error {
	if b.ch != nil {
		err := b.ch.Close()
		if err != nil && err != amqp.ErrClosed {
			return ErrMessageMiddlewareClose
		}
		b.ch = nil
	}

	if b.conn != nil {
		err := b.conn.Close()
		if err != nil && err != amqp.ErrClosed {
			return ErrMessageMiddlewareClose
		}
		b.conn = nil
	}

	return nil
}

func (b *baseMiddleware) StopConsuming() error {
	if b.ch == nil {
		b.Close()
		return ErrMessageMiddlewareDisconnected
	}

	if b.consumerTag != "" {
		err := b.ch.Cancel(b.consumerTag, false)
		if err != nil {
			return ErrMessageMiddlewareDisconnected
		}
	}

	if b.cancel != nil {
		b.cancel()
	}

	b.wg.Wait()
	return nil
}

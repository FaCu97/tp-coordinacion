package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func connect(connectionSettings ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()

	if err != nil {
		conn.Close()
		return nil, nil, ErrMessageMiddlewareDisconnected
	}
	return conn, ch, nil
}

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	conn, ch, err := connect(connectionSettings)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // args
	)

	if err != nil {
		ch.Close()
		conn.Close()
		return nil, ErrMessageMiddlewareMessage
	}

	queue := NewQueueMiddleware(conn, ch, q.Name)

	return queue, nil
}

func CreateExchangeMiddleware(exchangeName string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	conn, ch, err := connect(connectionSettings)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type
		true,         // durability
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, ErrMessageMiddlewareMessage
	}

	exchange := NewExchangeMiddleware(conn, ch, exchangeName, keys)

	return exchange, nil
}

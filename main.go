package main

import (
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	uri         string = "amqp://testuser:testpassword@localhost:5672/Some_Virtual_Host"
	exchange    string = "some_exchange"
	queueName   string = "some_outgoing_queue"
	bindingKey  string = "some_routing_key"
	consumerTag string = "testuser"
)

var (
	Log    = log.New(os.Stdout, "[INFO] ", log.LstdFlags|log.Lmsgprefix)
	ErrLog = log.New(os.Stderr, "[ERROR] ", log.LstdFlags|log.Lmsgprefix)
)

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func NewConsumer(amqpURI string, exchange string, queueName string, key string, ctag string) (*Consumer, error) {
	c := &Consumer{
		conn:    nil,
		channel: nil,
		tag:     ctag,
		done:    make(chan error),
	}

	var err error

	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	config.Properties.SetClientConnectionName("example-consumer")
	Log.Printf("Connecting: %q", amqpURI)
	c.conn, err = amqp.DialConfig(amqpURI, config)
	if err != nil {
		return nil, fmt.Errorf("Failed: %s", err)
	}

	go func() {
		Log.Printf("Closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	Log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	if err = c.channel.QueueBind(
		queueName, // name of the queue
		key,       // bindingKey
		exchange,  // sourceExchange
		false,     // noWait
		nil,       // arguments
	); err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	Log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", c.tag)
	deliveries, err := c.channel.Consume(
		queueName, // name
		c.tag,     // consumerTag,
		true,      // autoAck
		false,     // exclusive
		false,     // noLocal
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	go handle(deliveries, c.done)

	return c, nil
}

// run forever
func handle(deliveries <-chan amqp.Delivery, done chan error) {
	cleanup := func() {
		Log.Printf("handle: deliveries channel closed")
		done <- nil
	}

	defer cleanup()

	for d := range deliveries {
		Log.Printf(
			"got %dB delivery: [%v] %q",
			len(d.Body),
			d.DeliveryTag,
			d.Body,
		)
	}
}

func main() {
	c, err := NewConsumer(uri, exchange, queueName, bindingKey, consumerTag)
	if err != nil {
		ErrLog.Fatalf("%s", err)
	}

	Log.Printf("running until Consumer is done")
	<-c.done
}

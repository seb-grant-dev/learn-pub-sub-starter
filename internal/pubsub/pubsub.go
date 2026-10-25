package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)



type SimpleQueueType string 

const (
	Durable SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

type AckType string
const (
	Ack AckType = "ack"
	NackRequeue AckType = "nackrequeue"
	NackDiscard AckType = "nackdiscard"

)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	ctx := context.Background()
	mandatory := false
	immediate := false
	rabbitOpts := amqp.Publishing{
		ContentType: "application/json",
		Body: jsonBytes,
	}

	err = ch.PublishWithContext(
		ctx,
		exchange,
		key,
		mandatory,
		immediate,
		rabbitOpts,
	)

	if err != nil {
		fmt.Printf("Error publishing message: %+v\n",err)
		os.Exit(1)
	}
	return nil
}

func DeclareAndBind (
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {

	clientChan, err := conn.Channel()
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	durable := queueType == Durable
	autoDelete := queueType == Transient
	exclusive := queueType == Transient
	noWait := false

	clientQueue, err := clientChan.QueueDeclare(queueName, durable, autoDelete, exclusive, noWait, nil)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	clientChan.QueueBind(queueName, key, exchange, noWait, nil)

	return clientChan, clientQueue, nil

}
func SubscribeJSON[T any](
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    queueType SimpleQueueType, // an enum to represent "durable" or "transient"
    handler func(T) AckType,
) error {

	clientChan, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		fmt.Printf("Error binding to channel: %s\n",err)
		return err
	}

	ch, err := clientChan.Consume(queueName,"",false, false, false, false, nil)
	if err != nil {
		fmt.Printf("Error consuming channel: %s\n",err)
		return err
	}

	go func() {
		for del := range ch {
			var m T
			err := json.Unmarshal(del.Body, &m)
			if err == nil {
				ackType := handler(m)
				fmt.Printf("Delivery ack type: %s\n",ackType)
				switch ackType {
				case Ack:
					del.Ack(false)
				case NackRequeue:
					del.Nack(false, true)
				case NackDiscard:
					del.Nack(false,false)
				}
				del.Ack(false)
			} else {
				fmt.Println("Error unmarshalling JSON:",err)
			}
		}
	}()

	return nil
}

package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
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
	}

	return nil










	fmt.Printf("%+v\n",jsonBytes)


	return nil
}

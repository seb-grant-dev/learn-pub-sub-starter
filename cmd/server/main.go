package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"fmt"
	"log"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {

	fmt.Println("Starting Peril server...")
	
	connStr := "amqp://guest:guest@localhost:5672/"
	connection,err  := amqp.Dial(connStr)
	if err != nil {
		fmt.Println("Error connecting to RabbitMQ")
		os.Exit(1)
	}
	defer connection.Close()

	msgChan, err := connection.Channel()
	if err != nil {
		fmt.Println("Error creating channel")
		os.Exit(1)
	}

	fmt.Println("Connected to localhost:5672")

	gamelogic.PrintServerHelp()

	err = pubsub.SubscribeGob(connection, routing.ExchangePerilDirect, routing.GameLogSlug, fmt.Sprintf("%s.*",routing.GameLogSlug),pubsub.Durable, HandlerGameLog())
	if err != nil {
		log.Fatalf("Fatal: Error subscribing to Gob: %s",err)
	}


	inputLoop:
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "pause":
			fmt.Println("Pausing...")
			pubsub.PublishJSON(msgChan,routing.ExchangePerilDirect,routing.PauseKey,routing.PlayingState{IsPaused:true})
		case "resume":
			fmt.Println("Resuming...")
			pubsub.PublishJSON(msgChan,routing.ExchangePerilDirect,routing.PauseKey,routing.PlayingState{IsPaused:false})
		case "help":
			gamelogic.PrintServerHelp()
		case "quit":
			fmt.Println("Exiting...")
			break inputLoop
		default:
			fmt.Printf("Unknown command: %s\n",input[0])
		}
	}

}

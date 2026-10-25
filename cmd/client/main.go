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
	fmt.Println("Starting Peril client...")
	connStr := "amqp://guest:guest@localhost:5672/"
	connection,err  := amqp.Dial(connStr)
	if err != nil {
		fmt.Println("Error connecting to RabbitMQ")
		os.Exit(1)
	}
	defer connection.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Error: Username not provided.")
	}

	gameState := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s",routing.PauseKey, username),routing.PauseKey,pubsub.Transient, HandlerPause(gameState))
	if err != nil {
		log.Fatalf("Fatal: Error subscribing to JSON: %s",err)
	}

	fmt.Println("Connected to localhost:5672")

	clientChan, err := connection.Channel()
	if err != nil {
		log.Fatalf("Fatal: Could not connect to game: %s",err)
	}

	// Subscribe to Moves queue
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s",routing.ArmyMovesPrefix, username),fmt.Sprintf("%s.*",routing.ArmyMovesPrefix), pubsub.Transient, HandlerArmyMove(gameState))


	inputLoop:
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "spawn":
			gameState.CommandSpawn(input)
		case "move":
			move, err := gameState.CommandMove(input)
			if err == nil {
				fmt.Println("Move successful")
				pubErr := pubsub.PublishJSON(clientChan,routing.ExchangePerilTopic,fmt.Sprintf("%s.%s",routing.ArmyMovesPrefix,username),move)
				if pubErr != nil {
					fmt.Println("Error publishing the move")
				} else {
					fmt.Println("Move published")
				}
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			break inputLoop
		default:
			fmt.Printf("Unknown command: %s\n",input[0])
			continue
		}
	}
}

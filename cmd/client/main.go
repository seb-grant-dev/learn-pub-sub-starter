package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"fmt"
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

	pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s",routing.PauseKey,username), routing.PauseKey, pubsub.Transient)

	gameState := gamelogic.NewGameState(username)

	fmt.Println("Connected to localhost:5672")

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "spawn":
			gameState.CommandSpawn(input)
		case "move":
			_, err := gameState.CommandMove(input)
			if err == nil {
				fmt.Println("Move successful")
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
		default:
			fmt.Printf("Unknown command: %s\n",input[0])
			continue
		}
	}
}

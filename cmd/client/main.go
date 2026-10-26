package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

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
	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s",routing.ArmyMovesPrefix, username),
		fmt.Sprintf("%s.*",routing.ArmyMovesPrefix),
		pubsub.Transient,
		HandlerArmyMove(gameState, clientChan),
	)
	// Subscribe to War queue
	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		fmt.Sprintf("%s.*",routing.WarRecognitionsPrefix),
		pubsub.Durable,
		HandlerWar(gameState, clientChan),
	)

	if err != nil {
		fmt.Printf("Fatal: %s\n",err)
		os.Exit(1)
	}

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
			if len(input) > 1 {
				count, err := strconv.Atoi(input[1])
				if err != nil {
					fmt.Println("Try again: 'spam <count>'")
				} else {
					for range count {
						logObj := gamelogic.GetMaliciousLog()
						fmt.Println("Sending spam message",logObj)
						pubsub.PublishGob(clientChan, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s",routing.GameLogSlug, username),routing.GameLog{
							Username: username,
							Message: logObj,
							CurrentTime: time.Now(),
						})
					}
				}
			}
		case "quit":
			gamelogic.PrintQuit()
			break inputLoop
		default:
			fmt.Printf("Unknown command: %s\n",input[0])
			continue
		}
	}
}

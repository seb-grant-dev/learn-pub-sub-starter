package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"fmt"
	"os"
	"os/signal"
	
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


	fmt.Println("Connected to localhost:5672")
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	sig := <- signalChan
	if sig.String() == "interrupt" {

		fmt.Println("Shutting down...")
		signal.Stop(signalChan)
	}
}

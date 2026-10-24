package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"fmt"
	"os"
	"os/signal"
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

	fmt.Println("Connected to localhost:5672")
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	sig := <- signalChan
	if sig.String() == "interrupt" {

		fmt.Println("Shutting down...")
		signal.Stop(signalChan)
	}
}

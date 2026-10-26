package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func HandlerGameLog() func(routing.GameLog) pubsub.AckType{

	return func(gamelog routing.GameLog) pubsub.AckType {
		defer fmt.Println("> ")

		gamelogic.WriteLog(gamelog)

		return pubsub.Ack
	}
}

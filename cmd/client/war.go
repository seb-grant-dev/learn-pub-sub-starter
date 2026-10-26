package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	amqp "github.com/rabbitmq/amqp091-go"
)

func HandlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {

	return func(msg gamelogic.RecognitionOfWar) pubsub.AckType {
		
		defer fmt.Println("> ")

		outcome, winner, loser := gs.HandleWar(msg)
		
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			logMsg := fmt.Sprintf("%s won a war against %s",winner,loser)
			err := pubsub.PublishGameLog(ch, logMsg, gs.GetUsername())
			if err != nil {
				fmt.Println("Error publishing log: ",err)
				return pubsub.NackRequeue
			}
			fmt.Println("Published log.")
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			logMsg := fmt.Sprintf("%s won a war against %s",winner,loser)
			pubsub.PublishGameLog(ch, logMsg, gs.GetUsername())
			err := pubsub.PublishGameLog(ch, logMsg, gs.GetUsername())
			if err != nil {
				fmt.Println("Error publishing log: ",err)
				return pubsub.NackRequeue
			}

			fmt.Println("Published log.")
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			logMsg := fmt.Sprintf("A war between %s and %s resulted in a draw",winner,loser)
			pubsub.PublishGameLog(ch, logMsg, gs.GetUsername())
			err := pubsub.PublishGameLog(ch, logMsg, gs.GetUsername())
			if err != nil {
				fmt.Println("Error publishing log: ",err)
				return pubsub.NackRequeue
			}
			fmt.Println("Published log.")
			return pubsub.Ack
		default:
			fmt.Println("Error processing War data")
			return pubsub.NackDiscard
		}

	}
}

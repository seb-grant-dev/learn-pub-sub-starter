package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
)

func HandlerArmyMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove){
	defer fmt.Printf("> ")

	return func(mv gamelogic.ArmyMove){
		gs.HandleMove(mv)
	}
}

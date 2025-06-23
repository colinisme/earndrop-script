package main

import (
	"context"
	"fmt"
	"generate_merkle_tree_sample/generatesvc"
	_ "github.com/lib/pq"
)

func main() {
	earndropId := int64(500000)
	stages := []generatesvc.DBEarndropStage{
		{
			ID:          1,
			EarndropID:  earndropId,
			Index:       0,
			UnlockRatio: float64(0.44444444),
		},
		{
			ID:          2,
			EarndropID:  earndropId,
			Index:       1,
			UnlockRatio: float64(0.09259259),
		},
		{
			ID:          3,
			EarndropID:  earndropId,
			Index:       2,
			UnlockRatio: float64(0.09259259),
		},
		{
			ID:          4,
			EarndropID:  earndropId,
			Index:       3,
			UnlockRatio: float64(0.09259259),
		},
		{
			ID:          5,
			EarndropID:  earndropId,
			Index:       4,
			UnlockRatio: float64(0.09259259),
		},
		{
			ID:          6,
			EarndropID:  earndropId,
			Index:       5,
			UnlockRatio: float64(0.09259259),
		},
		{
			ID:          7,
			EarndropID:  earndropId,
			Index:       6,
			UnlockRatio: float64(0.09259259),
		},
	}

	var ratio float64
	for _, stage := range stages {
		ratio += stage.UnlockRatio
	}

	fmt.Println(ratio)

	earndropSvc := generatesvc.NewService(earndropId, stages, "./sahara-airdrop-5.csv")

	ctx := context.Background()
	_ = earndropSvc.InitEarndropData(ctx)
}

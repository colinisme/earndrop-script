package generatesvc

import (
	"context"
	"github.com/rs/zerolog/log"
	"math/big"
)

type service struct {
	earndropId  int64
	csvFilePath string
	stages      []DBEarndropStage
}

func NewService(
	earndropId int64,
	stages []DBEarndropStage,
	csvFilePath string,
) Service {
	return &service{
		earndropId:  earndropId,
		stages:      stages,
		csvFilePath: csvFilePath,
	}
}

type Service interface {
	InitEarndropData(ctx context.Context) error
}

func (s *service) InitEarndropData(ctx context.Context) error {

	earndropFileData, err := s.downloadEarndropFileDataOptimized(ctx)
	if err != nil {
		log.Err(err).Msg("failed to download earndrop file data")
		return err
	}
	totalOriginalAmount := big.NewInt(0)
	for _, userData := range earndropFileData {
		totalOriginalAmount = new(big.Int).Add(totalOriginalAmount, userData.TokenAmount)
	}
	log.Info().Str("totalOriginalAmount", totalOriginalAmount.String()). // 449998874437000000000000000
										Int("length", len(earndropFileData)). // 1410307
										Msg("earndrop data loaded")

	err = s.initEarndropRelateDbDataOptimized(ctx, s.stages, earndropFileData)
	if err != nil {
		log.Err(err).Msg("failed to init earndrop relate db data")
		return err
	}

	return nil
}

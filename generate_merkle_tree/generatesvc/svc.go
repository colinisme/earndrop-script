package generatesvc

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/stumble/wpgx"
)

type service struct {
	dbConnPool *wpgx.Pool

	earndropId int64
}

func NewService(
	dbConnPool *wpgx.Pool,
	earndropId int64,
) Service {
	return &service{
		earndropId: earndropId,
		dbConnPool: dbConnPool,
	}
}

type Service interface {
	InitEarndropData(ctx context.Context) error
}

func (s *service) InitEarndropData(ctx context.Context) error {
	earndropInfo, err := s.getEarndropInfo(ctx)
	if err != nil {
		log.Err(err).Msg("failed to get earndrop info")
		return err
	}
	if earndropInfo == nil {
		log.Error().Msg("earndrop info is nil")
		return errors.New("earndrop info is nil")
	}
	if earndropInfo.DistributionInfo.FilePath == "" {
		log.Error().Msg("earndrop file path is empty")
		return errors.New("earndrop file path is empty")
	}

	earndropStages, err := s.getEarndropStages(ctx)
	if err != nil {
		log.Err(err).Msg("failed to get earndrop stages")
		return err
	}

	if err := s.validateEarndropStageInfo(earndropStages); err != nil {
		log.Err(err).Msg("failed to validate earndrop stages")
		return err
	}

	earndropFileData, err := s.downloadEarndropFileDataOptimized(ctx, earndropInfo.DistributionInfo.FilePath)
	if err != nil {
		log.Err(err).Msg("failed to download earndrop file data")
		return err
	}

	newEarndropData, err := s.initEarndropRelateDbDataOptimized(ctx, earndropInfo, earndropStages, earndropFileData)
	if err != nil {
		log.Err(err).Msg("failed to init earndrop relate db data")
		return err
	}

	log.Info().Interface("newEarndropData", newEarndropData).Interface("earndropStages", earndropStages).Msg("earndrop info")

	return nil
}

func (s *service) getEarndropInfo(ctx context.Context) (*DBEarndropInfo, error) {
	qctx, cancel := context.WithTimeout(ctx, time.Millisecond*2000)
	defer cancel()

	sql := `select id, chain, alias, merkle_tree_hash_root, distribution_info from earndrop where id=$1 limit 1`

	row := s.dbConnPool.WConn().WQueryRow(qctx, "earndrop.GetEarndropInfo", sql, s.earndropId)

	var i = new(DBEarndropInfo)
	err := row.Scan(
		&i.ID,
		&i.Chain,
		&i.Alias,
		&i.MerkleTreeHashRoot,
		&i.DistributionInfo,
	)
	if err == pgx.ErrNoRows {
		return (*DBEarndropInfo)(nil), nil
	} else if err != nil {
		return nil, err
	}

	return i, err
}

func (s *service) getEarndropStages(ctx context.Context) ([]DBEarndropStage, error) {
	qctx, cancel := context.WithTimeout(ctx, time.Millisecond*2000)
	defer cancel()

	sql := `select
		  id, earndrop_id, index, start_at, end_at, unlock_ratio
		from
		  earndrop_stage
		where
		  earndrop_id = $1
		order by
		  index asc`

	rows, err := s.dbConnPool.WConn().WQuery(qctx, "earndrop.GetEarndropStages", sql, s.earndropId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []DBEarndropStage
	for rows.Next() {
		var i = new(DBEarndropStage)
		if err := rows.Scan(
			&i.ID,
			&i.EarndropID,
			&i.Index,
			&i.StartAt,
			&i.EndAt,
			&i.UnlockRatio,
		); err != nil {
			return nil, err
		}
		items = append(items, *i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, err

}

func (s *service) validateEarndropStageInfo(stageList []DBEarndropStage) error {
	if len(stageList) == 0 {
		return errors.New("stage list is empty")
	}

	var totalRatio float64

	// validate stage data
	for _, stage := range stageList {
		if stage.StartAt.IsZero() {
			return errors.New("invalid start time")
		}
		if stage.EndAt.IsZero() {
			return errors.New("invalid end time")
		}
		if stage.StartAt.After(stage.EndAt) {
			return errors.New("start time is after end time")
		}

		totalRatio += stage.UnlockRatio
	}

	return nil
}

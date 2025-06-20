package generatesvc

import (
	"math/big"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

type DistributionInfo struct {
	FilePath     string `json:"file_path"`
	IsEarndropV2 bool   `json:"is_earndrop_v2"`
}

type DBEarndropInfo struct {
	ID                 int64  `json:"id"`
	Chain              string `json:"chain"`
	Alias              string `json:"alias"`
	MerkleTreeHashRoot string `json:"merkle_tree_hash_root"`

	DistributionInfo DistributionInfo `json:"distribution_info"`
}

type DBEarndropStage struct {
	ID          int64     `json:"id"`
	EarndropID  int64     `json:"earndrop_id"`
	Index       int64     `json:"index"`
	StartAt     time.Time `json:"start_at"`
	EndAt       time.Time `json:"end_at"`
	UnlockRatio float64   `json:"unlock_ratio"`
}

type EarndropClaimDetailStatus string

const (
	EarndropClaimDetailStatusGenerated EarndropClaimDetailStatus = "Generated"
	EarndropClaimDetailStatusPending   EarndropClaimDetailStatus = "Pending"
	EarndropClaimDetailStatusClaimed   EarndropClaimDetailStatus = "Claimed"
)

type DBEarndropClaimDetail struct {
	EarndropID         int64                     `json:"earndrop_id"`
	EarndropStageID    int64                     `json:"earndrop_stage_id"`
	LeafIndex          int64                     `json:"leaf_index"`
	UserAddress        string                    `json:"user_address"`
	Amount             *big.Int                  `json:"amount"`
	Proof              []string                  `json:"proof"`
	Status             EarndropClaimDetailStatus `json:"status"`
	ClaimFee           pgtype.Numeric            `json:"claim_fee"`
	EarndropStageIndex int64                     `json:"earndrop_stage_index"`
}

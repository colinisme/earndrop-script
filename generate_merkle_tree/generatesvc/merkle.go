package generatesvc

import (
	"math/big"

	"github.com/NFTGalaxy/rick/pkg/to"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	mt "github.com/txaty/go-merkletree"
)

type EarndropUserData struct {
	Index       int64
	UserAddress string
	TokenAmount *big.Int
}

type merkleDataBlock struct {
	EarndropId      *big.Int
	EarndropStageId *big.Int
	LeafIndex       *big.Int
	UserAddress     common.Address
	Amount          *big.Int
}

func (d *merkleDataBlock) Serialize() ([]byte, error) {

	var packed []byte
	packed = append(packed, math.U256Bytes(d.EarndropId)...)
	packed = append(packed, math.U256Bytes(d.EarndropStageId)...)
	packed = append(packed, math.U256Bytes(d.LeafIndex)...)
	packed = append(packed, d.UserAddress.Bytes()...)
	packed = append(packed, math.U256Bytes(d.Amount)...)

	return packed, nil
}

func newMerkleDataBlock(claimDetail *DBEarndropClaimDetail, isEarndropV2 bool, stageIndex int64) (mt.DataBlock, error) {
	amount := to.MustNumericToBigInt(claimDetail.Amount)

	if isEarndropV2 {
		return &merkleDataBlockV2{
			EarndropId:         big.NewInt(claimDetail.EarndropID),
			EarndropStageIndex: big.NewInt(stageIndex),
			LeafIndex:          big.NewInt(claimDetail.LeafIndex),
			UserAddress:        common.HexToAddress(claimDetail.UserAddress),
			Amount:             amount,
		}, nil
	}

	return &merkleDataBlock{
		EarndropId:      big.NewInt(claimDetail.EarndropID),
		EarndropStageId: big.NewInt(claimDetail.EarndropStageID),
		LeafIndex:       big.NewInt(claimDetail.LeafIndex),
		UserAddress:     common.HexToAddress(claimDetail.UserAddress),
		Amount:          amount,
	}, nil
}

type merkleDataBlockV2 struct {
	EarndropId         *big.Int
	EarndropStageIndex *big.Int
	LeafIndex          *big.Int
	UserAddress        common.Address
	Amount             *big.Int
}

func (d *merkleDataBlockV2) Serialize() ([]byte, error) {

	var packed []byte
	packed = append(packed, math.U256Bytes(d.EarndropId)...)
	packed = append(packed, math.U256Bytes(d.EarndropStageIndex)...)
	packed = append(packed, math.U256Bytes(d.LeafIndex)...)
	packed = append(packed, d.UserAddress.Bytes()...)
	packed = append(packed, math.U256Bytes(d.Amount)...)

	return packed, nil
}

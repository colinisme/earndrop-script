package generatesvc

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	mt "github.com/txaty/go-merkletree"
)

// DataBlockBatch 用于批量处理的数据结构
type DataBlockBatch struct {
	Data []mt.DataBlock
	Err  error
}

func (s *service) initEarndropMerkleTreeOptimized(
	ctx context.Context,
	claimDetailList []*DBEarndropClaimDetail,
	isEarndropV2 bool,
	stageIndexMap map[int64]int64,
) (*mt.MerkleTree, error) {

	startTime := time.Now()
	log.Info().Int("totalRecords", len(claimDetailList)).Msg("starting merkle tree generation")

	// 1. 并发生成data blocks
	leafDataBlocks, err := s.generateDataBlocksConcurrent(
		ctx,
		claimDetailList,
		isEarndropV2,
		stageIndexMap,
	)
	if err != nil {
		return nil, err
	}

	log.Info().Int("dataBlocks", len(leafDataBlocks)).Float64("elapsed", time.Since(startTime).Seconds()).Msg("data blocks generated")

	// To match the Rust implementation, we must sort the leaf nodes by their hash before building the tree.
	hashFunc := func(data []byte) ([]byte, error) {
		hash := crypto.Keccak256Hash(data)
		return hash.Bytes(), nil
	}

	log.Info().Msg("sorting leaf nodes by hash")

	// 2. Merkle树创建
	treeStartTime := time.Now()
	tree, err := mt.New(&mt.Config{
		HashFunc:         hashFunc,
		Mode:             mt.ModeTreeBuild,
		SortSiblingPairs: true,
		RunInParallel:    true,
		NumRoutines:      50,
	}, leafDataBlocks)

	if err != nil {
		return nil, errors.WithMessage(err, "failed to init merkle tree")
	}

	//leafDataBlocks = nil
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	log.Info().Float64("treeBuildTime", time.Since(treeStartTime).Seconds()).
		Float64("totalTime", time.Since(startTime).Seconds()).
		Uint64("allocMB", m.Alloc/1024/1024).
		Uint64("sysMB", m.Sys/1024/1024).
		Uint32("numGC", m.NumGC).
		Msg("merkle tree built successfully")

	return tree, nil
}

// generateDataBlocksConcurrent 并发生成data blocks
func (s *service) generateDataBlocksConcurrent(
	ctx context.Context,
	claimDetailList []*DBEarndropClaimDetail,
	isEarndropV2 bool,
	stageIndexMap map[int64]int64,
) ([]mt.DataBlock, error) {

	numWorkers := 1
	batchSize := len(claimDetailList) / numWorkers
	if batchSize < 1000 {
		batchSize = 1000
	}

	indexChan := make(chan int, numWorkers*2)
	batchChan := make(chan DataBlockBatch, numWorkers)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go s.generateDataBlocksWorker(
			ctx,
			&wg,
			claimDetailList,
			isEarndropV2,
			stageIndexMap,
			batchSize,
			indexChan,
			batchChan,
		)
	}

	go func() {
		defer close(indexChan)
		for i := 0; i < len(claimDetailList); i++ {
			select {
			case indexChan <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	// 合并结果
	var allDataBlocks []mt.DataBlock
	completedBatches := 0

	for batch := range batchChan {
		if batch.Err != nil {
			return nil, batch.Err
		}

		allDataBlocks = append(allDataBlocks, batch.Data...)
		completedBatches++

		if completedBatches%10 == 0 {
			log.Info().Int("completedBatches", completedBatches).Int("totalDataBlocks", len(allDataBlocks)).Msg("data blocks generation progress")
		}
	}

	return allDataBlocks, nil
}

// generateDataBlocksWorker 生成data blocks的worker
func (s *service) generateDataBlocksWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	claimDetailList []*DBEarndropClaimDetail,
	isEarndropV2 bool,
	stageIndexMap map[int64]int64,
	batchSize int,
	indexChan <-chan int,
	batchChan chan<- DataBlockBatch,
) {
	defer wg.Done()

	var batch []mt.DataBlock
	var indices []int

	for index := range indexChan {
		select {
		case <-ctx.Done():
			return
		default:
		}

		indices = append(indices, index)

		if len(indices) >= batchSize {
			dataBlocks, err := s.processDataBlockBatch(claimDetailList, indices, isEarndropV2, stageIndexMap)
			if err != nil {
				select {
				case batchChan <- DataBlockBatch{Err: err}:
				default:
				}
				return
			}

			batch = append(batch, dataBlocks...)
			indices = indices[:0] // 清空indices
		}
	}

	// 处理剩余的索引
	if len(indices) > 0 {
		dataBlocks, err := s.processDataBlockBatch(claimDetailList, indices, isEarndropV2, stageIndexMap)
		if err != nil {
			select {
			case batchChan <- DataBlockBatch{Err: err}:
			default:
			}
			return
		}

		batch = append(batch, dataBlocks...)
	}

	select {
	case batchChan <- DataBlockBatch{Data: batch}:
	case <-ctx.Done():
		return
	}
}

// processDataBlockBatch 批量处理data blocks
func (s *service) processDataBlockBatch(
	claimDetailList []*DBEarndropClaimDetail,
	indices []int,
	isEarndropV2 bool,
	stageIndexMap map[int64]int64,
) ([]mt.DataBlock, error) {

	dataBlocks := make([]mt.DataBlock, 0, len(indices))

	for _, index := range indices {
		claimData := claimDetailList[index]
		dataBlock, err := newMerkleDataBlock(claimData, isEarndropV2, stageIndexMap[claimData.EarndropStageID])
		if err != nil {
			return nil, errors.WithMessage(err, "failed to create merkle data block")
		}
		dataBlocks = append(dataBlocks, dataBlock)
	}

	return dataBlocks, nil
}

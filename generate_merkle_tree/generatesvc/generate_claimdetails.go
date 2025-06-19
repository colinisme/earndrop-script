package generatesvc

import (
	"context"
	"errors"
	"math/big"
	"runtime"
	"strings"
	"sync"
	"time"

	rickChain "github.com/NFTGalaxy/rick/pkg/chain"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"
)

func (s *service) generateClaimDetailsOptimized(
	ctx context.Context,
	earndropData *DBEarndropInfo,
	earndropStageData []DBEarndropStage,
	earndropFileData []*EarndropUserData,
	stageUnlockRatios map[int64]*big.Int,
) ([]*DBEarndropClaimDetail, error) {

	startTime := time.Now()
	totalUsers := len(earndropFileData)
	totalStages := len(earndropStageData)
	expectedTotal := totalUsers * totalStages

	log.Info().
		Int("totalUsers", totalUsers).
		Int("totalStages", totalStages).
		Int("expectedClaimDetails", expectedTotal).
		Msg("starting memory-optimized claim details generation")

	allClaimDetails := make([]*DBEarndropClaimDetail, 0, expectedTotal)

	numWorkers := runtime.NumCPU() * 6 // 减少worker数量
	batchSize := 5000                  // 更小的批次大小

	log.Info().
		Int("numWorkers", numWorkers).
		Int("batchSize", batchSize).
		Int("numBatches", (totalUsers+batchSize-1)/batchSize).
		Msg("memory-optimized configuration")

	userDataChan := make(chan []*EarndropUserData, numWorkers)
	resultChan := make(chan []*DBEarndropClaimDetail, numWorkers)

	// 启动worker goroutines
	var wg sync.WaitGroup
	workerStartTime := time.Now()
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go s.generateClaimDetailsWorkerOptimized(
			ctx,
			i,
			&wg,
			earndropData,
			earndropStageData,
			stageUnlockRatios,
			userDataChan,
			resultChan,
		)
	}

	log.Info().
		Float64("workerStartupTime", time.Since(workerStartTime).Seconds()).
		Msg("all  workers started")

	// 分批分发用户数据，避免一次性加载太多数据到内存
	dataDistributionStart := time.Now()
	go func() {
		defer close(userDataChan)
		batchCount := 0
		for i := 0; i < totalUsers; i += batchSize {
			end := i + batchSize
			if end > totalUsers {
				end = totalUsers
			}
			batchCount++
			select {
			case userDataChan <- earndropFileData[i:end]:
			case <-ctx.Done():
				log.Warn().Msg("data distribution canceled due to context cancellation")
				return
			}

			time.Sleep(time.Millisecond * 1)
		}
		log.Info().
			Int("totalBatches", batchCount).
			Float64("distributionTime", time.Since(dataDistributionStart).Seconds()).
			Msg("claim-detail data distribution completed")
	}()

	// 收集结果
	resultCollectionStart := time.Now()
	go func() {
		wg.Wait()
		close(resultChan)
		log.Info().
			Float64("resultCollectionTime", time.Since(resultCollectionStart).Seconds()).
			Msg("result collection completed")
	}()

	resultMergeStart := time.Now()
	leafIndex := int64(0)
	completedBatches := 0
	totalClaimDetails := 0

	for batch := range resultChan {
		// 更新leaf index
		for _, detail := range batch {
			detail.LeafIndex = leafIndex
			leafIndex++
		}

		allClaimDetails = append(allClaimDetails, batch...)
		completedBatches++
		totalClaimDetails += len(batch)

		// 每完成50个批次记录一次进度
		if completedBatches%50 == 0 {
			// 强制GC，释放内存
			runtime.GC()

			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			log.Info().
				Int("completedBatches", completedBatches).
				Int("totalClaimDetails", totalClaimDetails).
				Int64("currentLeafIndex", leafIndex).
				Float64("elapsed", time.Since(startTime).Seconds()).
				Float64("mergeTime", time.Since(resultMergeStart).Seconds()).
				Uint64("allocMB", m.Alloc/1024/1024).
				Uint64("sysMB", m.Sys/1024/1024).
				Uint32("numGC", m.NumGC).
				Msg("memory-optimized claim details generation progress")
		}
	}

	log.Info().
		Int("completedBatches", completedBatches).
		Int("totalClaimDetails", totalClaimDetails).
		Int("expectedTotal", expectedTotal).
		Float64("totalTime", time.Since(startTime).Seconds()).
		Float64("mergeTime", time.Since(resultMergeStart).Seconds()).
		Float64("avgTimePerClaimDetail", time.Since(startTime).Seconds()/float64(totalClaimDetails)).
		Msg("claim details generation completed")

	if totalClaimDetails != expectedTotal {
		log.Warn().
			Int("actual", totalClaimDetails).
			Int("expected", expectedTotal).
			Int("difference", expectedTotal-totalClaimDetails).
			Msg("claim details count mismatch")

		return nil, errors.New("total details count mismatch")
	}

	return allClaimDetails, nil
}

func (s *service) generateClaimDetailsWorkerOptimized(
	ctx context.Context,
	workerId int,
	wg *sync.WaitGroup,
	earndropData *DBEarndropInfo,
	earndropStageData []DBEarndropStage,
	stageUnlockRatios map[int64]*big.Int,
	userDataChan <-chan []*EarndropUserData,
	resultChan chan<- []*DBEarndropClaimDetail,
) {
	defer wg.Done()

	processedBatches := 0
	totalProcessedUsers := 0
	workerStartTime := time.Now()

	for userDataBatch := range userDataChan {
		select {
		case <-ctx.Done():
			log.Warn().Msg("memory-optimized worker canceled due to context cancellation")
			return
		default:
		}

		batchStartTime := time.Now()
		batchSize := len(userDataBatch)

		expectedClaimDetails := batchSize * len(earndropStageData)
		batch := make([]*DBEarndropClaimDetail, 0, expectedClaimDetails)

		for _, userData := range userDataBatch {
			userAddress := strings.ToLower(userData.UserAddress)
			if earndropData.Chain == rickChain.Solana.String() || earndropData.Chain == rickChain.Solana_Devnet.String() {
				userAddress = userData.UserAddress
			}

			totalAmount := userData.TokenAmount

			// 为每个stage生成claim detail
			for _, stage := range earndropStageData {
				unlockRatioBig := stageUnlockRatios[stage.ID]
				unlockAmount := new(big.Int).Mul(totalAmount, unlockRatioBig)
				unlockAmount = unlockAmount.Div(unlockAmount, big.NewInt(1e8))

				claimDetail := &DBEarndropClaimDetail{
					EarndropID:      earndropData.ID,
					EarndropStageID: stage.ID,
					LeafIndex:       0, // 将在后续更新
					UserAddress:     userAddress,
					Amount: pgtype.Numeric{
						Int:   unlockAmount,
						Valid: true,
					},
					Proof:  nil, // will be set later
					Status: EarndropClaimDetailStatusGenerated,
				}
				batch = append(batch, claimDetail)
			}
		}

		processedBatches++
		totalProcessedUsers += batchSize

		// 记录进度
		if processedBatches%100000 == 0 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			log.Info().
				Int("workerId", workerId).
				Int("processedBatches", processedBatches).
				Int("totalProcessedUsers", totalProcessedUsers).
				Int("currentBatchSize", batchSize).
				Int("claimDetailsInBatch", len(batch)).
				Float64("batchTime", time.Since(batchStartTime).Seconds()).
				Float64("totalWorkerTime", time.Since(workerStartTime).Seconds()).
				Float64("avgTimePerBatch", time.Since(workerStartTime).Seconds()/float64(processedBatches)).
				Uint64("allocMB", m.Alloc/1024/1024).
				Msg("generate-claim-detail worker progress")
		}

		select {
		case resultChan <- batch:
		case <-ctx.Done():
			log.Warn().Msg("worker result sending canceled due to context cancellation")
			return
		}
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	log.Info().
		Int("workerId", workerId).
		Int("totalProcessedBatches", processedBatches).
		Int("totalProcessedUsers", totalProcessedUsers).
		Float64("totalWorkerTime", time.Since(workerStartTime).Seconds()).
		Float64("avgTimePerUser", time.Since(workerStartTime).Seconds()/float64(totalProcessedUsers)).
		Uint64("finalAllocMB", m.Alloc/1024/1024).
		Msg("generate-claim-detail worker completed")
}

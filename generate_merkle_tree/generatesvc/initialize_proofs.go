package generatesvc

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NFTGalaxy/rick/pkg/to"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	mt "github.com/txaty/go-merkletree"
)

// ProofResult 存储proof结果的数据结构
type ProofResult struct {
	ClaimDetailID int64
	Proof         [][]byte

	Index int
}

type ProofBatch struct {
	Index        int
	ClaimDetails []*DBEarndropClaimDetail

	InsertParams []*DBEarndropClaimDetail

	Processed bool
}

// csvWriter 线程安全的CSV写入器
type csvWriter struct {
	filepath string
	mu       sync.Mutex
	file     *os.File
	writer   *csv.Writer
}

func newCSVWriter(filepath string) *csvWriter {
	return &csvWriter{
		filepath: filepath,
	}
}

func (cw *csvWriter) writeHeaders(headers []string) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	file, err := os.Create(cw.filepath)
	if err != nil {
		return err
	}
	cw.file = file
	cw.writer = csv.NewWriter(file)

	return cw.writer.Write(headers)
}

func (cw *csvWriter) writeRows(rows [][]string) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	// 如果文件还没创建，先创建
	if cw.file == nil {
		file, err := os.Create(cw.filepath)
		if err != nil {
			return err
		}
		cw.file = file
		cw.writer = csv.NewWriter(file)
	}

	// 写入所有行
	for _, row := range rows {
		if err := cw.writer.Write(row); err != nil {
			return err
		}
	}

	// 立即刷新到磁盘
	cw.writer.Flush()
	return cw.writer.Error()
}

func (cw *csvWriter) close() error {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	if cw.writer != nil {
		cw.writer.Flush()
	}
	if cw.file != nil {
		return cw.file.Close()
	}
	return nil
}

func (s *service) generateAndSaveMerkleProofs(
	ctx context.Context,
	claimDetailList []*DBEarndropClaimDetail,
	merkleTree *mt.MerkleTree,
	isEarndropV2 bool,
	stageIndexMap map[int64]int64,
	outputDir string,
) (string, error) {
	startTime := time.Now()
	totalClaimDetails := len(claimDetailList)

	log.Info().
		Int("totalClaimDetails", totalClaimDetails).
		Msg("starting merkle proofs generation")

	var csvWriter *csvWriter
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", errors.WithMessage(err, "failed to create output directory")
	}

	prefixFileName := fmt.Sprintf("earndrop_claim_details_%s", time.Now().Format("20060102_150405"))
	filename := fmt.Sprintf("%s.csv", prefixFileName)
	filepath := filepath.Join(outputDir, filename)

	csvWriter = newCSVWriter(filepath)

	headers := []string{
		"earndrop_id",
		"earndrop_stage_id",
		"leaf_index",
		"user_address",
		"amount",
		"proof",
		"status",
		"earndrop_stage_index",
	}
	if err := csvWriter.writeHeaders(headers); err != nil {
		return "", errors.WithMessage(err, "failed to write CSV headers")
	}

	log.Info().Str("filepath", filepath).Msg("CSV file created and headers written")

	batchSize := 5000
	numWorkers := 1

	log.Info().
		Int("numWorkers", numWorkers).
		Int("batchSize", batchSize).
		Int("numBatches", (totalClaimDetails+batchSize-1)/batchSize).
		Msg(" proof generation configuration")

	batchChan := make(chan *ProofBatch, numWorkers*50)
	resultChan := make(chan *ProofBatch, numWorkers*50)
	errorChan := make(chan error, 1)

	var wg sync.WaitGroup
	workerStartTime := time.Now()
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go s.generateAndSaveProofsWorker(
			ctx,
			&wg,
			merkleTree,
			isEarndropV2,
			stageIndexMap,
			batchChan,
			resultChan,
			errorChan,
		)
	}

	log.Info().
		Float64("workerStartupTime", time.Since(workerStartTime).Seconds()).
		Msg("all  proof workers started")

	batchDistributionStart := time.Now()
	go func() {
		defer close(batchChan)
		batchIndex := 0

		for i := 0; i < totalClaimDetails; i += batchSize {
			end := i + batchSize
			if end > totalClaimDetails {
				end = totalClaimDetails
			}

			batch := &ProofBatch{
				Index:        batchIndex,
				ClaimDetails: make([]*DBEarndropClaimDetail, end-i),
				InsertParams: make([]*DBEarndropClaimDetail, end-i),
			}

			copy(batch.ClaimDetails, claimDetailList[i:end])

			select {
			case batchChan <- batch:
				batchIndex++
			case <-ctx.Done():
				log.Warn().Msg("batch distribution canceled due to context cancellation")
				return
			}

			if batchIndex%10 == 0 {
				runtime.GC()
				time.Sleep(time.Millisecond * 10)
			}
		}

		log.Info().
			Int("totalBatches", batchIndex).
			Float64("distributionTime", time.Since(batchDistributionStart).Seconds()).
			Msg("proof generation batch distribution completed")
	}()

	// 收集所有结果
	//var allResults []*ProofResult
	var totalProcessed int64
	var processedBatches int64
	resultStartTime := time.Now()

	var resultWg sync.WaitGroup
	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		defer func() {
			// 关闭CSV写入器
			if csvWriter != nil {
				if err := csvWriter.close(); err != nil {
					log.Error().Err(err).Msg("failed to close CSV writer")
				}
			}
		}()

		for batch := range resultChan {
			atomic.AddInt64(&totalProcessed, int64(len(batch.InsertParams)))
			atomic.AddInt64(&processedBatches, 1)

			if err := s.exportToCSVOptimized(ctx, batch.InsertParams, csvWriter, int(atomic.LoadInt64(&processedBatches))); err != nil {
				log.Error().Err(err).Msg("failed to export batch to CSV file")
			}

			// 立即释放批次内存
			batch.ClaimDetails = nil
			batch.InsertParams = nil
			batch = nil

			// 每处理100个批次记录一次进度
			if atomic.LoadInt64(&processedBatches)%100 == 0 {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)

				log.Info().
					Int64("processedBatches", atomic.LoadInt64(&processedBatches)).
					Int64("totalProcessedUsers", atomic.LoadInt64(&totalProcessed)).
					Float64("progress", float64(atomic.LoadInt64(&totalProcessed))/float64(totalClaimDetails)*100).
					Float64("resultTime", time.Since(resultStartTime).Seconds()).
					Uint64("allocMB", m.Alloc/1024/1024).
					Msg("proof result processing progress")
			}

			// 每处理50个批次强制GC
			if atomic.LoadInt64(&processedBatches)%50 == 0 {
				runtime.GC()
				time.Sleep(time.Millisecond * 5)
			}
		}
	}()

	waitStartTime := time.Now()
	go func() {
		wg.Wait()
		log.Info().Msg("all workers completed, closing errorChan and resultChan")
		close(errorChan)
		close(resultChan) // 在workers完成后关闭resultChan
		log.Info().
			Float64("waitTime", time.Since(waitStartTime).Seconds()).
			Msg(" proof generation workers completed")
	}()

	// 等待所有workers完成后再检查错误
	errorCheckStart := time.Now()
	errorCount := 0
	log.Info().Msg("starting to check for errors...")
	for err := range errorChan {
		if err != nil {
			errorCount++
			log.Error().Err(err).Int("errorCount", errorCount).Msg("proof generation error")
			return "", err
		}
	}
	log.Info().Msg("error checking completed")

	// 等待结果处理goroutine完成
	log.Info().Msg("waiting for result processing to complete...")
	resultWg.Wait()
	log.Info().Msg("result processing completed")

	runtime.GC()
	var finalMemStats runtime.MemStats
	runtime.ReadMemStats(&finalMemStats)

	log.Info().
		Int("errorCount", errorCount).
		Int64("totalProcessed", atomic.LoadInt64(&totalProcessed)).
		Int64("totalBatches", atomic.LoadInt64(&processedBatches)).
		Float64("errorCheckTime", time.Since(errorCheckStart).Seconds()).
		Float64("totalTime", time.Since(startTime).Seconds()).
		Float64("avgTimePerProof", time.Since(startTime).Seconds()/float64(totalClaimDetails)).
		Uint64("finalAllocMB", finalMemStats.Alloc/1024/1024).
		Uint64("finalSysMB", finalMemStats.Sys/1024/1024).
		Uint32("numGC", finalMemStats.NumGC).
		Msg("proofs generation completed")

	// 数据完整性验证
	expectedCount := int64(totalClaimDetails)
	actualCount := atomic.LoadInt64(&totalProcessed)
	if actualCount != expectedCount {
		log.Warn().
			Int64("expectedCount", expectedCount).
			Int64("actualCount", actualCount).
			Int64("missingCount", expectedCount-actualCount).
			Float64("completionRate", float64(actualCount)/float64(expectedCount)*100).
			Msg("data integrity check: some claim details were not processed successfully")
	} else {
		log.Info().
			Int64("processedCount", actualCount).
			Msg("data integrity check: all claim details processed successfully")
	}

	return prefixFileName, nil
}

func (s *service) generateAndSaveProofsWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	merkleTree *mt.MerkleTree,
	isEarndropV2 bool,
	stageIndexMap map[int64]int64,
	batchChan <-chan *ProofBatch,
	resultChan chan<- *ProofBatch,
	errorChan chan<- error,
) {
	defer wg.Done()

	var processedCount int64
	workerStartTime := time.Now()

	for batch := range batchChan {
		select {
		case <-ctx.Done():
			log.Warn().Msg("memory-optimized-v3 proof worker canceled due to context cancellation")
			return
		default:
		}

		// 处理批次中的每个proof
		for i, claimDetail := range batch.ClaimDetails {
			stageIndex := stageIndexMap[claimDetail.EarndropStageID]
			dataBlock, err := newMerkleDataBlock(claimDetail, isEarndropV2, stageIndex)
			if err != nil {
				log.Error().
					Err(err).
					Int64("earndropID", claimDetail.EarndropID).
					Int64("earndropStageID", claimDetail.EarndropStageID).
					Int64("leafIndex", claimDetail.LeafIndex).
					Str("userAddress", claimDetail.UserAddress).
					Msg("failed to create merkle data block, skipping this claim detail")
				continue // 跳过这个claim detail，继续处理下一个
			}

			proof, err := merkleTree.Proof(dataBlock)
			if err != nil {
				log.Error().
					Err(err).
					Int64("earndropID", claimDetail.EarndropID).
					Int64("earndropStageID", claimDetail.EarndropStageID).
					Int64("leafIndex", claimDetail.LeafIndex).
					Str("userAddress", claimDetail.UserAddress).
					Msg("failed to get merkle proof, skipping this claim detail")
				continue // 跳过这个claim detail，继续处理下一个
			}

			proofSlice := make([]string, len(proof.Siblings))
			for j, p := range proof.Siblings {
				proofSlice[j] = hexutil.Encode(p)
			}

			batch.InsertParams[i] = &DBEarndropClaimDetail{
				EarndropID:         claimDetail.EarndropID,
				EarndropStageID:    claimDetail.EarndropStageID,
				LeafIndex:          claimDetail.LeafIndex,
				UserAddress:        claimDetail.UserAddress,
				Amount:             claimDetail.Amount,
				Proof:              proofSlice,
				Status:             EarndropClaimDetailStatusGenerated,
				EarndropStageIndex: stageIndex,
			}

			atomic.AddInt64(&processedCount, 1)
		}

		batch.Processed = true

		// 处理完成后发送到resultChan中
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
		Int64("totalProcessedProofs", atomic.LoadInt64(&processedCount)).
		Float64("totalWorkerTime", time.Since(workerStartTime).Seconds()).
		Float64("avgTimePerProof", time.Since(workerStartTime).Seconds()/float64(atomic.LoadInt64(&processedCount))).
		Uint64("finalAllocMB", m.Alloc/1024/1024).
		Msg("memory-optimized-v3 proof worker completed")
}

// exportToCSVOptimized 使用优化的CSV写入器导出数据
func (s *service) exportToCSVOptimized(ctx context.Context, insertParams []*DBEarndropClaimDetail, csvWriter *csvWriter, batchIndex int) error {
	// 准备所有行数据
	var rows [][]string
	var skippedCount int
	for _, param := range insertParams {
		// 跳过nil的参数（处理失败的claim detail）
		if param == nil {
			skippedCount++
			continue
		}

		proofStr := fmt.Sprintf("{%s}", strings.Join(param.Proof, ","))

		amount := to.MustNumericToBigInt(param.Amount)

		row := []string{
			strconv.FormatInt(param.EarndropID, 10),
			strconv.FormatInt(param.EarndropStageID, 10),
			strconv.FormatInt(param.LeafIndex, 10),
			param.UserAddress,
			amount.String(),
			proofStr,
			string(param.Status),
			strconv.FormatInt(param.EarndropStageIndex, 10),
		}

		rows = append(rows, row)
	}

	// 批量写入所有行
	if err := csvWriter.writeRows(rows); err != nil {
		return errors.WithMessage(err, "failed to write CSV rows")
	}

	log.Info().
		Int("batchIndex", batchIndex).
		Int("recordCount", len(insertParams)).
		Int("exportedCount", len(rows)).
		Int("skippedCount", skippedCount).
		Msg("successfully exported batch to CSV file")

	return nil
}

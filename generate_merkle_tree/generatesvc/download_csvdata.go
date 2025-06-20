package generatesvc

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// EarndropUserDataBatch 用于批量处理的数据结构
type EarndropUserDataBatch struct {
	Data []*EarndropUserData
	Err  error
}

func (s *service) downloadEarndropFileDataOptimized(ctx context.Context, filePath string) ([]*EarndropUserData, error) {
	if filePath == "" {
		return nil, errors.New("file path is empty")
	}

	log.Info().Str("file", filePath).Msg("downloading earndrop file ")

	client := &http.Client{
		Timeout: time.Hour * 1, // 增加超时时间
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, filePath, nil)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create request")
	}

	fileResp, err := client.Do(req)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get earndrop file")
	}
	if fileResp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("failed to get earndrop file with status: %d", fileResp.StatusCode)
	}
	defer func() {
		_ = fileResp.Body.Close()
	}()

	log.Info().Msg("downloaded earndrop file")

	bufferedReader := bufio.NewReaderSize(fileResp.Body, 1024*1024*10)
	reader := csv.NewReader(bufferedReader)

	numWorkers := 1
	batchSize := 40000

	recordChan := make(chan []string, batchSize*2)
	batchChan := make(chan EarndropUserDataBatch, numWorkers)
	resultChan := make(chan []*EarndropUserData, 1)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go s.processRecordsWorker(
			ctx,
			&wg,
			recordChan,
			batchChan,
			batchSize,
		)
	}

	go s.collectResults(ctx, batchChan, resultChan, numWorkers)

	// const maxRecords = 100000
	go func() {
		defer close(recordChan)
		var count int64
		startTime := time.Now()

		for {
			// if count >= maxRecords {
			// 	log.Info().Int64("processedRecords", count).Dur("elapsed", time.Since(startTime)).Msg("reached records limit, stopping")
			// 	break
			// }

			record, err := reader.Read()
			if err != nil {
				if err == io.EOF {
					log.Info().Int64("processedRecords", count).Dur("elapsed", time.Since(startTime)).Msg("reached end of file")
					break
				} else {
					log.Error().Err(err).Msg("failed to read csv file")
					return
				}
			}

			select {
			case recordChan <- record:
				count++
				if count%100000 == 0 {
					log.Info().Int64("processedRecords", count).Dur("elapsed", time.Since(startTime)).Msg("reading progress")
				}
			case <-ctx.Done():
				return
			}
		}

		log.Info().Int64("totalRecords", count).Dur("elapsed", time.Since(startTime)).Msg("finished reading csv file")
	}()

	wg.Wait()
	close(batchChan)

	select {
	case result := <-resultChan:
		log.Info().Int("finalRecordCount", len(result)).Msg("successfully processed records ")
		return result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// processRecordsWorker 处理记录的worker
func (s *service) processRecordsWorker(ctx context.Context, wg *sync.WaitGroup, recordChan <-chan []string, batchChan chan<- EarndropUserDataBatch, batchSize int) {
	defer wg.Done()

	var batch []*EarndropUserData
	indexMap := make(map[int64]struct{})
	addressMap := make(map[string]struct{})

	for record := range recordChan {
		select {
		case <-ctx.Done():
			return
		default:
		}

		data, err := s.parseEarndropFileData(record)
		if err != nil {
			batchChan <- EarndropUserDataBatch{Err: errors.WithMessage(err, "failed to parse earndrop file data")}
			return
		}

		// 检查index是否重复
		if _, ok := indexMap[data.Index]; ok {
			batchChan <- EarndropUserDataBatch{Err: errors.Errorf("duplicate index found in earndrop file, %d", data.Index)}
			return
		}
		indexMap[data.Index] = struct{}{}

		lowerAddress := strings.ToLower(data.UserAddress) // 这里转成小写去判断数据是否重复
		if _, ok := addressMap[lowerAddress]; ok {
			batchChan <- EarndropUserDataBatch{Err: errors.Errorf("duplicate address found in earndrop file, %s", data.UserAddress)}
			return
		}
		addressMap[lowerAddress] = struct{}{}

		batch = append(batch, data)

		// 当批次满了就发送
		if len(batch) >= batchSize {
			select {
			case batchChan <- EarndropUserDataBatch{Data: batch}:
				batch = make([]*EarndropUserData, 0, batchSize)
			case <-ctx.Done():
				return
			}
		}
	}

	// 发送剩余的批次
	if len(batch) > 0 {
		select {
		case batchChan <- EarndropUserDataBatch{Data: batch}:
		case <-ctx.Done():
			return
		}
	}
}

func (s *service) collectResults(ctx context.Context, batchChan <-chan EarndropUserDataBatch, resultChan chan<- []*EarndropUserData, numWorkers int) {
	var allData []*EarndropUserData
	completedWorkers := 0

	for batch := range batchChan {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if batch.Err != nil {
			log.Error().Err(batch.Err).Msg("error in batch processing")
			resultChan <- nil
			return
		}

		allData = append(allData, batch.Data...)
		completedWorkers++

		if completedWorkers%10 == 0 {
			log.Info().Int("completedBatches", completedWorkers).Int("totalRecords", len(allData)).Msg("collection progress")
		}
	}

	log.Info().Int("totalRecords", len(allData)).Msg("finished collecting all data")
	resultChan <- allData
}

func (s *service) initEarndropRelateDbDataOptimized(
	ctx context.Context,
	earndropData *DBEarndropInfo,
	earndropStageData []DBEarndropStage,
	earndropFileData []*EarndropUserData,
) (*DBEarndropInfo, error) {

	startTime := time.Now()
	log.Info().Int("stages", len(earndropStageData)).Int("users", len(earndropFileData)).Msg("starting optimized earndrop data initialization")

	stageIndexMap := make(map[int64]int64)
	stageUnlockRatios := make(map[int64]*big.Int)

	for _, stage := range earndropStageData {
		stageIndexMap[stage.ID] = stage.Index
		stageUnlockRatios[stage.ID] = new(big.Int).SetInt64(int64(stage.UnlockRatio * 1e12))
	}

	claimDetailList, err := s.generateClaimDetailsOptimized(
		ctx,
		earndropData,
		earndropStageData,
		earndropFileData,
		stageUnlockRatios,
	)
	if err != nil {
		return nil, err
	}

	log.Info().Int("claimDetails", len(claimDetailList)).Dur("elapsed", time.Since(startTime)).Msg("claim details generated")

	merkleTree, err := s.initEarndropMerkleTreeOptimized(ctx, claimDetailList, earndropData.DistributionInfo.IsEarndropV2, stageIndexMap)
	if err != nil {
		log.Err(err).Msg("failed to init earndrop merkle tree")
		return nil, errors.WithMessage(err, "failed to init earndrop merkle tree")
	}

	// 4. 更新earndrop数据
	newEarndropData := earndropData
	newEarndropData.MerkleTreeHashRoot = hexutil.Encode(merkleTree.Root)
	log.Info().Float64("elapsed", time.Since(startTime).Seconds()).Any("earndropData", earndropData).Msg("merkle tree initialized")

	// 5. 并发生成Merkle proofs 并且存到db里面
	prefixFileName, err := s.generateAndSaveMerkleProofs(ctx, claimDetailList, merkleTree, earndropData.DistributionInfo.IsEarndropV2, stageIndexMap, ".")
	if err != nil {
		return nil, err
	}

	log.Info().Dur("totalElapsed", time.Since(startTime)).Msg("merkle proofs generated")

	// 6. 保存新的earndrop数据到文件
	if err := s.saveEarndropData(prefixFileName, newEarndropData); err != nil {
		log.Err(err).Msg("failed to save new earndrop data")
		//return nil, errors.WithMessage(err, "failed to save new earndrop data")
	}

	return newEarndropData, nil
}

func (s *service) parseEarndropFileData(record []string) (*EarndropUserData, error) {
	if len(record) != 3 {
		return nil, errors.New("invalid record length")
	}

	indexData, addressData, amountData := record[0], record[1], record[2]

	// validate record data
	index, err := strconv.ParseInt(indexData, 10, 64)
	if err != nil {
		return nil, errors.Errorf("invalid index: %s, %v", indexData, err)
	}

	amount, ok := new(big.Int).SetString(amountData, 10)
	if !ok {
		return nil, errors.Errorf("invalid amount: %s", amountData)
	}

	return &EarndropUserData{
		Index:       index,
		UserAddress: addressData,
		TokenAmount: amount,
	}, nil
}

func (s *service) saveEarndropData(prefixFileName string, earndropData *DBEarndropInfo) error {
	fileName := fmt.Sprintf("%s.json", prefixFileName)
	file, err := os.Create(fileName)
	if err != nil {
		log.Err(err).Str("fileName", fileName).Msg("failed to create file")
		return errors.WithMessage(err, "failed to create file")
	}
	defer file.Close() // nolint

	data, err := json.MarshalIndent(earndropData, "", "  ")
	if err != nil {
		log.Err(err).Msg("failed to marshal newEarndropData")
		return errors.WithMessage(err, "failed to marshal newEarndropData")
	}

	if _, err := file.Write(data); err != nil {
		log.Err(err).Str("fileName", fileName).Msg("failed to write to file")
		return errors.WithMessage(err, "failed to write to file")
	}

	log.Info().Str("fileName", fileName).Msg("newEarndropData successfully written to file")
	return nil
}

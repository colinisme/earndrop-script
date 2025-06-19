package validatesvc

import (
	"context"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/rs/zerolog/log"
)

// Service 提供Merkle proof验证服务
type Service struct{}

// NewService 创建新的验证服务实例
func NewService() *Service {
	return &Service{}
}

// ValidationResult 验证结果
type ValidationResult struct {
	TotalRecords   int               `json:"totalRecords"`
	ValidProofs    int               `json:"validProofs"`
	InvalidProofs  int               `json:"invalidProofs"`
	ErrorRecords   int               `json:"errorRecords"`
	ValidationTime time.Duration     `json:"validationTime"`
	Errors         []ValidationError `json:"errors"`
}

// ValidationError 验证错误
type ValidationError struct {
	RowIndex int    `json:"rowIndex"`
	Error    string `json:"error"`
	Record   string `json:"record"`
}

// CSVRecord CSV记录结构
type CSVRecord struct {
	EarndropID         int64  `json:"earndropId"`
	EarndropStageID    int64  `json:"earndropStageId"`
	LeafIndex          int64  `json:"leafIndex"`
	UserAddress        string `json:"userAddress"`
	Amount             string `json:"amount"`
	Proof              string `json:"proof"`
	Status             string `json:"status"`
	EarndropStageIndex int64  `json:"earndropStageIndex"`
}

// ValidateMerkleProofs 验证CSV文件中的Merkle proofs
func (s *Service) ValidateMerkleProofs(ctx context.Context, csvFile, merkleRoot string) (*ValidationResult, error) {
	startTime := time.Now()

	// 解析Merkle根
	root, err := hex.DecodeString(strings.TrimPrefix(merkleRoot, "0x"))
	if err != nil {
		return nil, fmt.Errorf("invalid merkle root: %w", err)
	}

	// 读取CSV文件
	records, err := s.readCSVFile(csvFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV file: %w", err)
	}

	log.Info().Int("totalRecords", len(records)).Msg("开始验证Merkle proofs")

	// 并发验证
	result := s.validateRecordsConcurrently(ctx, records, root)
	result.ValidationTime = time.Since(startTime)

	return result, nil
}

// readCSVFile 读取CSV文件
func (s *Service) readCSVFile(filename string) ([]CSVRecord, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// 读取标题行
	_, err = reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	var records []CSVRecord
	rowIndex := 1 // 从1开始，因为0是标题行

	for {
		rowIndex++
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading row %d: %w", rowIndex, err)
		}

		if len(row) < 8 {
			continue // 跳过不完整的行
		}

		record, err := s.parseCSVRow(row, rowIndex)
		if err != nil {
			log.Warn().Err(err).Int("row", rowIndex).Msg("跳过无效行")
			continue
		}

		records = append(records, record)
	}

	return records, nil
}

// parseCSVRow 解析CSV行
func (s *Service) parseCSVRow(row []string, rowIndex int) (CSVRecord, error) {
	earndropID, err := strconv.ParseInt(row[0], 10, 64)
	if err != nil {
		return CSVRecord{}, fmt.Errorf("invalid earndrop_id: %s", row[0])
	}

	earndropStageID, err := strconv.ParseInt(row[1], 10, 64)
	if err != nil {
		return CSVRecord{}, fmt.Errorf("invalid earndrop_stage_id: %s", row[1])
	}

	leafIndex, err := strconv.ParseInt(row[2], 10, 64)
	if err != nil {
		return CSVRecord{}, fmt.Errorf("invalid leaf_index: %s", row[2])
	}

	userAddress := strings.TrimSpace(row[3])
	if !common.IsHexAddress(userAddress) {
		return CSVRecord{}, fmt.Errorf("invalid user_address: %s", userAddress)
	}

	amount := strings.TrimSpace(row[4])
	if amount == "" {
		return CSVRecord{}, fmt.Errorf("empty amount")
	}

	proof := strings.TrimSpace(row[5])
	if proof == "" {
		return CSVRecord{}, fmt.Errorf("empty proof")
	}

	status := strings.TrimSpace(row[6])

	earndropStageIndex, err := strconv.ParseInt(row[7], 10, 64)
	if err != nil {
		return CSVRecord{}, fmt.Errorf("invalid earndrop_stage_index: %s", row[7])
	}

	return CSVRecord{
		EarndropID:         earndropID,
		EarndropStageID:    earndropStageID,
		LeafIndex:          leafIndex,
		UserAddress:        userAddress,
		Amount:             amount,
		Proof:              proof,
		Status:             status,
		EarndropStageIndex: earndropStageIndex,
	}, nil
}

// validateRecordsConcurrently 并发验证记录
func (s *Service) validateRecordsConcurrently(ctx context.Context, records []CSVRecord, merkleRoot []byte) *ValidationResult {
	result := &ValidationResult{
		TotalRecords: len(records),
		Errors:       make([]ValidationError, 0),
	}

	// 使用工作池进行并发验证
	numWorkers := 8 // 可以根据需要调整
	recordChan := make(chan recordWithIndex, len(records))
	resultChan := make(chan validationResult, len(records))

	// 启动工作协程
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for recordWithIdx := range recordChan {
				select {
				case <-ctx.Done():
					return
				default:
					resultChan <- s.validateSingleRecord(recordWithIdx.record, merkleRoot, recordWithIdx.rowIndex)
				}
			}
		}()
	}

	// 发送记录到工作协程
	go func() {
		defer close(recordChan)
		for i, record := range records {
			select {
			case <-ctx.Done():
				return
			case recordChan <- recordWithIndex{record: record, rowIndex: i + 2}: // +2 因为第1行是标题，从第2行开始
			}
		}
	}()

	// 收集结果
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 处理结果
	for res := range resultChan {
		if res.IsValid {
			result.ValidProofs++
		} else {
			result.InvalidProofs++
		}

		if res.Error != nil {
			result.ErrorRecords++
			result.Errors = append(result.Errors, ValidationError{
				RowIndex: res.RowIndex,
				Error:    res.Error.Error(),
				Record:   res.RecordString,
			})
		}
	}

	return result
}

// recordWithIndex 带行号的记录
type recordWithIndex struct {
	record   CSVRecord
	rowIndex int
}

// validationResult 单个验证结果
type validationResult struct {
	IsValid      bool
	Error        error
	RowIndex     int
	RecordString string
}

// validateSingleRecord 验证单个记录
func (s *Service) validateSingleRecord(record CSVRecord, merkleRoot []byte, rowIndex int) validationResult {
	// 生成叶子节点哈希
	leaf, err := s.generateLeafHash(record)
	if err != nil {
		return validationResult{
			IsValid:      false,
			Error:        fmt.Errorf("failed to generate leaf hash: %w", err),
			RowIndex:     rowIndex,
			RecordString: fmt.Sprintf("%+v", record),
		}
	}

	// 解析proof
	proof, err := s.parseProof(record.Proof)
	if err != nil {
		return validationResult{
			IsValid:      false,
			Error:        fmt.Errorf("failed to parse proof: %w", err),
			RowIndex:     rowIndex,
			RecordString: fmt.Sprintf("%+v", record),
		}
	}

	// 验证proof
	isValid := s.verifyMerkleProof(leaf, proof, merkleRoot)

	return validationResult{
		IsValid:      isValid,
		Error:        nil,
		RowIndex:     rowIndex,
		RecordString: fmt.Sprintf("%+v", record),
	}
}

// generateLeafHash 生成叶子节点哈希
// 对应合约中的: keccak256(abi.encodePacked(earndropId, params.stageIndex, params.leafIndex, params.account, params.amount))
func (s *Service) generateLeafHash(record CSVRecord) ([]byte, error) {
	// 将amount转换为big.Int
	amount := new(big.Int)
	amount.SetString(record.Amount, 10)

	// 编码数据
	data := make([]byte, 0)

	// earndropId (uint256)
	earndropIDBytes := make([]byte, 32)
	big.NewInt(record.EarndropID).FillBytes(earndropIDBytes)
	data = append(data, earndropIDBytes...)

	// stageIndex (uint256)
	stageIndexBytes := make([]byte, 32)
	big.NewInt(record.EarndropStageIndex).FillBytes(stageIndexBytes)
	data = append(data, stageIndexBytes...)

	// leafIndex (uint256)
	leafIndexBytes := make([]byte, 32)
	big.NewInt(record.LeafIndex).FillBytes(leafIndexBytes)
	data = append(data, leafIndexBytes...)

	// account (address)
	accountBytes := common.FromHex(record.UserAddress)
	data = append(data, accountBytes...)

	// amount (uint256)
	amountBytes := make([]byte, 32)
	amount.FillBytes(amountBytes)
	data = append(data, amountBytes...)

	// 计算keccak256哈希
	hash := crypto.Keccak256(data)
	return hash, nil
}

// parseProof 解析proof字符串
func (s *Service) parseProof(proofStr string) ([][]byte, error) {
	// 移除花括号和空格
	proofStr = strings.Trim(proofStr, "{}")
	proofStr = strings.ReplaceAll(proofStr, " ", "")

	// 分割proof元素
	elements := strings.Split(proofStr, ",")

	var proof [][]byte
	for _, element := range elements {
		// 移除0x前缀
		element = strings.TrimPrefix(element, "0x")

		// 解码hex
		bytes, err := hex.DecodeString(element)
		if err != nil {
			return nil, fmt.Errorf("invalid proof element %s: %w", element, err)
		}

		proof = append(proof, bytes)
	}

	return proof, nil
}

// verifyMerkleProof 验证Merkle proof
func (s *Service) verifyMerkleProof(leaf []byte, proof [][]byte, root []byte) bool {
	current := leaf

	for _, proofElement := range proof {
		// 比较当前节点和proof元素，决定连接顺序
		if bytesLess(current, proofElement) {
			// current + proofElement
			current = crypto.Keccak256(current, proofElement)
		} else {
			// proofElement + current
			current = crypto.Keccak256(proofElement, current)
		}
	}

	// 比较最终结果与根哈希
	return bytesEqual(current, root)
}

// bytesLess 比较两个字节数组
func bytesLess(a, b []byte) bool {
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// bytesEqual 比较两个字节数组是否相等
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

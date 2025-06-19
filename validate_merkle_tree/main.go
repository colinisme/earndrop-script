package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"validate_merkle_tree/validatesvc"
)

func main() {
	// 解析命令行参数
	var (
		csvFile    = flag.String("csv", "", "CSV文件路径 (必需)")
		merkleRoot = flag.String("root", "", "Merkle根哈希 (必需)")
		outputFile = flag.String("output", "", "输出文件路径 (可选，默认输出到控制台)")
		verbose    = flag.Bool("verbose", false, "详细输出模式")
		numWorkers = flag.Int("workers", 0, "并发worker数量 (默认: CPU核心数)")
		batchSize  = flag.Int("batch", 1000, "批处理大小 (默认: 1000)")
		timeout    = flag.Duration("timeout", 0, "超时时间 (例如: 30m)")
		showHelp   = flag.Bool("help", false, "显示帮助信息")
	)

	flag.Parse()

	// 显示帮助信息
	if *showHelp {
		fmt.Println("Merkle Proof 验证工具")
		fmt.Println()
		fmt.Println("用法:")
		fmt.Println("  validate -csv <CSV文件> -root <Merkle根> [选项]")
		fmt.Println()
		fmt.Println("必需参数:")
		fmt.Println("  -csv string    CSV文件路径")
		fmt.Println("  -root string   Merkle根哈希")
		fmt.Println()
		fmt.Println("可选参数:")
		fmt.Println("  -output string     输出文件路径 (默认输出到控制台)")
		fmt.Println("  -verbose           详细输出模式")
		fmt.Println("  -workers int       并发worker数量 (默认: CPU核心数)")
		fmt.Println("  -batch int         批处理大小 (默认: 1000)")
		fmt.Println("  -timeout duration  超时时间 (例如: 30m)")
		fmt.Println("  -help              显示此帮助信息")
		fmt.Println()
		fmt.Println("示例:")
		fmt.Println("  validate -csv proofs.csv -root 0x1234... -verbose")
		fmt.Println("  validate -csv large_proofs.csv -root 0x1234... -workers 8 -batch 2000 -timeout 1h")
		fmt.Println("  validate -csv proofs.csv -root 0x1234... -output results.txt")
		return
	}

	// 验证必需参数
	if *csvFile == "" {
		log.Fatal().Msg("CSV文件路径是必需的，请使用 -csv 参数指定")
	}
	if *merkleRoot == "" {
		log.Fatal().Msg("Merkle根哈希是必需的，请使用 -root 参数指定")
	}

	// 设置日志级别
	if *verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// 设置默认worker数量
	if *numWorkers <= 0 {
		*numWorkers = runtime.NumCPU() // 使用CPU核心数，避免过多并发
	}

	// 限制最大worker数量，避免过多并发导致问题
	if *numWorkers > 16 {
		log.Warn().Int("requestedWorkers", *numWorkers).Int("maxWorkers", 16).Msg("limiting worker count to prevent excessive concurrency")
		*numWorkers = 16
	}

	// 显示配置信息
	log.Info().
		Str("csvFile", *csvFile).
		Str("merkleRoot", *merkleRoot).
		Int("numWorkers", *numWorkers).
		Int("batchSize", *batchSize).
		Int("numCPU", runtime.NumCPU()).
		Msg("validation configuration")

	// 检查文件是否存在
	if _, err := os.Stat(*csvFile); os.IsNotExist(err) {
		log.Fatal().Str("file", *csvFile).Msg("CSV文件不存在")
	}

	// 创建上下文
	ctx := context.Background()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
		log.Info().Dur("timeout", *timeout).Msg("validation timeout set")
	}

	// 创建验证服务
	service := validatesvc.NewService()

	// 开始验证
	startTime := time.Now()
	log.Info().Msg("开始验证Merkle proofs...")

	result, err := service.ValidateMerkleProofs(ctx, *csvFile, *merkleRoot)
	if err != nil {
		log.Fatal().Err(err).Msg("验证失败")
	}

	// 输出结果
	outputResult(result, *outputFile, *verbose)

	// 输出性能统计
	totalTime := time.Since(startTime)
	log.Info().
		Float64("totalTime", totalTime.Seconds()).
		Float64("recordsPerSecond", float64(result.TotalRecords)/totalTime.Seconds()).
		Msg("validation performance summary")
}

// outputResult 输出验证结果
func outputResult(result *validatesvc.ValidationResult, outputFile string, verbose bool) {
	// 准备输出内容
	output := fmt.Sprintf(`Merkle Proof 验证结果
=====================
总记录数: %d
有效Proof: %d
无效Proof: %d
错误记录: %d
验证时间: %.2f秒
处理速度: %.2f 记录/秒
成功率: %.2f%%

`,
		result.TotalRecords,
		result.ValidProofs,
		result.InvalidProofs,
		result.ErrorRecords,
		result.ValidationTime.Seconds(),
		float64(result.TotalRecords)/result.ValidationTime.Seconds(),
		float64(result.ValidProofs)/float64(result.TotalRecords)*100,
	)

	// 如果有错误，添加错误详情
	if len(result.Errors) > 0 {
		output += fmt.Sprintf("错误详情 (显示前%d个):\n", min(len(result.Errors), 10))
		output += "=====================\n"

		maxErrors := len(result.Errors)
		if !verbose && maxErrors > 10 {
			maxErrors = 10
		}

		for i := 0; i < maxErrors; i++ {
			output += fmt.Sprintf("行 %d: %s\n", result.Errors[i].RowIndex, result.Errors[i].Error)
			if verbose {
				output += fmt.Sprintf("  记录: %s\n", result.Errors[i].Record)
			}
		}

		if !verbose && len(result.Errors) > 10 {
			output += fmt.Sprintf("... 还有 %d 个错误未显示 (使用 -verbose 查看全部)\n", len(result.Errors)-10)
		}
	}

	// 输出到文件或控制台
	if outputFile != "" {
		err := os.WriteFile(outputFile, []byte(output), 0644)
		if err != nil {
			log.Error().Err(err).Str("file", outputFile).Msg("写入输出文件失败")
			fmt.Print(output) // 回退到控制台输出
		} else {
			log.Info().Str("file", outputFile).Msg("结果已保存到文件")
		}
	} else {
		fmt.Print(output)
	}
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

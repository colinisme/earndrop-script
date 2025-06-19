package main

import (
	"crypto/rand"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
)

// 生成随机的EVM地址
func generateRandomEVMAddress() string {
	// EVM地址是20字节，以0x开头
	bytes := make([]byte, 20)
	_, err := rand.Read(bytes)
	if err != nil {
		log.Fatal("生成随机字节失败:", err)
	}

	// 转换为十六进制字符串
	address := "0x"
	for _, b := range bytes {
		address += fmt.Sprintf("%02x", b)
	}

	return address
}

// 生成不重复的EVM地址集合
func generateUniqueEVMAddresses(count int) []string {
	addresses := make(map[string]bool)
	addressSlice := make([]string, 0, count)

	attempts := 0
	maxAttempts := count * 10 // 设置最大尝试次数，避免无限循环

	fmt.Printf("开始生成不重复的EVM地址...\n")

	for len(addressSlice) < count && attempts < maxAttempts {
		address := generateRandomEVMAddress()

		// 检查地址是否已存在
		if !addresses[address] {
			addresses[address] = true
			addressSlice = append(addressSlice, address)

			// 每生成1000个地址显示一次进度
			if len(addressSlice)%1000 == 0 {
				fmt.Printf("已生成 %d 个不重复地址 (进度: %.2f%%)\n",
					len(addressSlice), float64(len(addressSlice))/float64(count)*100)
			}
		}
		attempts++
	}

	if len(addressSlice) < count {
		log.Fatalf("无法生成足够的唯一地址。已生成: %d, 需要: %d, 尝试次数: %d",
			len(addressSlice), count, attempts)
	}

	fmt.Printf("成功生成 %d 个不重复的EVM地址\n", len(addressSlice))
	return addressSlice
}

func main() {
	// 设置要生成的数据条数
	recordCount := 3000000

	// 读取claim_11.csv的数据
	fmt.Printf("读取claim_11.csv数据...\n")
	claimFile, err := os.Open("claim_11.csv")
	if err != nil {
		log.Fatal("打开claim_11.csv失败:", err)
	}
	defer claimFile.Close()

	claimReader := csv.NewReader(claimFile)
	claimRecords, err := claimReader.ReadAll()
	if err != nil {
		log.Fatal("读取claim_11.csv失败:", err)
	}

	fmt.Printf("claim_11.csv包含 %d 条记录\n", len(claimRecords))

	// 获取claim_11.csv中的最大ID
	maxClaimID := 0
	for _, record := range claimRecords {
		if len(record) > 0 {
			id, err := strconv.Atoi(record[0])
			if err == nil && id > maxClaimID {
				maxClaimID = id
			}
		}
	}
	fmt.Printf("claim_11.csv中最大ID为: %d\n", maxClaimID)
	fmt.Printf("新数据的ID将从 %d 开始\n", maxClaimID+1)

	// 生成不重复的EVM地址
	fmt.Printf("正在生成 %d 个不重复的EVM地址...\n", recordCount)
	addresses := generateUniqueEVMAddresses(recordCount)

	// 验证地址不重复
	fmt.Printf("验证地址唯一性...\n")
	addressSet := make(map[string]bool)
	for i, addr := range addresses {
		if addressSet[addr] {
			log.Fatalf("发现重复地址: %s (索引: %d)", addr, i)
		}
		addressSet[addr] = true
	}
	fmt.Printf("地址唯一性验证通过，共 %d 个唯一地址\n", len(addresses))

	// 创建CSV文件
	file, err := os.Create("data.csv")
	if err != nil {
		log.Fatal("创建CSV文件失败:", err)
	}
	defer file.Close()

	// 创建CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入CSV头部
	// header := []string{"ID", "EVM Address", "Amount"}
	// err = writer.Write(header)
	// if err != nil {
	// 	log.Fatal("写入CSV头部失败:", err)
	// }

	// 固定数值：10000000000000000
	fixedAmount := "10000000000000000"

	// 首先写入claim_11.csv的数据
	fmt.Printf("写入claim_11.csv的数据...\n")
	for _, record := range claimRecords {
		err = writer.Write(record)
		if err != nil {
			log.Fatal("写入claim_11.csv数据失败:", err)
		}
	}

	// 写入新生成的数据行
	fmt.Printf("正在写入 %d 条新数据到CSV文件...\n", recordCount)
	for i := 0; i < recordCount; i++ {
		record := []string{
			strconv.Itoa(maxClaimID + 1 + i), // 从最大ID+1开始的自增ID
			addresses[i],                     // 随机EVM地址
			fixedAmount,                      // 固定数值
		}

		err = writer.Write(record)
		if err != nil {
			log.Fatal("写入数据行失败:", err)
		}

		// 每写入10000条记录显示一次进度
		if (i+1)%10000 == 0 {
			fmt.Printf("已写入 %d 条新记录 (进度: %.2f%%)\n",
				i+1, float64(i+1)/float64(recordCount)*100)
		}
	}

	totalRecords := len(claimRecords) + recordCount
	fmt.Printf("成功生成CSV文件: data.csv\n")
	fmt.Printf("包含 %d 条claim_11.csv数据 + %d 条新数据 = 总计 %d 条数据\n",
		len(claimRecords), recordCount, totalRecords)
	fmt.Printf("ID范围: 1-%d (claim_11.csv) + %d-%d (新数据)\n",
		maxClaimID, maxClaimID+1, maxClaimID+recordCount)
	fmt.Printf("文件格式: ID, EVM Address, Amount\n")
}

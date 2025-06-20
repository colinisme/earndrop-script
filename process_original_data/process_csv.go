package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"math/big"
	"os"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: process_csv <input.csv> <output.csv>")
		return
	}
	inputFile := os.Args[1]
	outputFile := os.Args[2]

	in, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("failed to open input file: %v", err)
	}
	defer in.Close()

	reader := csv.NewReader(in)
	rows, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("failed to read csv: %v", err)
	}

	if len(rows) < 2 {
		log.Fatalf("csv should have at least one data row")
	}

	// Find column indices
	addressIdx, amountIdx := -1, -1
	for i, col := range rows[0] {
		if col == "address" {
			addressIdx = i
		}
		if col == "total" {
			amountIdx = i
		}
	}
	if addressIdx == -1 || amountIdx == -1 {
		log.Fatalf("csv header must contain 'address' and 'total'")
	}

	out, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("failed to create output file: %v", err)
	}
	defer out.Close()
	writer := csv.NewWriter(out)
	defer writer.Flush()

	// Write header
	// writer.Write([]string{"id", "address", "amount"})

	totalAmount := new(big.Int)
	minAmount := new(big.Int)
	minAddress := ""
	miniAmountIndex := 0
	miniAmountData := ""
	first := true

	for i, row := range rows[1:] {
		address := row[addressIdx]
		amountStr := row[amountIdx]

		amountInt, ok := new(big.Int).SetString(amountStr, 10)
		if !ok {
			log.Fatalf("invalid amount: %s", amountStr)
		}

		// amountRat := new(big.Rat)
		// _, ok := amountRat.SetString(amountStr)
		// if !ok {
		// 	log.Fatalf("invalid amount: %s", amountStr)
		// }
		// factor := new(big.Rat).SetFloat64(1e18)
		// amountRat.Mul(amountRat, factor)
		// amountInt := new(big.Int)
		// amountRat.FloatString(0) // 仅用于调试
		// amountRatNum := new(big.Float).SetRat(amountRat)
		// amountRatNum.Int(amountInt)
		writer.Write([]string{
			fmt.Sprintf("%d", i+1),
			address,
			amountInt.String(),
		})
		totalAmount.Add(totalAmount, amountInt)

		if first || amountInt.Cmp(minAmount) < 0 {
			minAmount.Set(amountInt)
			minAddress = address
			miniAmountData = amountStr
			miniAmountIndex = i
			first = false
		}
	}

	fmt.Println("Done! Output written to", outputFile)
	fmt.Println("Total amount:", totalAmount.String())
	fmt.Printf("Min amount: %s, address: %s, index: %d, data: %s\n", minAmount.String(), minAddress, miniAmountIndex, miniAmountData)
}

// Total amount: 449998874437000000000000000   449998874.437
// Min amount: 65000000000000000, address: 0x2effa03491367d88b59f03f05da508c41158c2d3, index: 136085, data: 65000000000000000

// 449998865437022511260000000
// 449998865.4370225

// 9872150
// 1410307*7=9872149

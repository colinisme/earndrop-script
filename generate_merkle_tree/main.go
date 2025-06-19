package main

import (
	"context"
	"flag"
	"log"
	"time"

	rickWpgx "github.com/NFTGalaxy/rick/pkg/database/wpgx"
	_ "github.com/lib/pq"
	"github.com/stumble/wpgx"

	"generate_merkle_tree/generatesvc"
)

func main() {
	var (
		earndropId = flag.Int64("earndrop-id", 0, "Earndrop ID to initialize")
	)
	flag.Parse()

	if *earndropId == 0 {
		log.Fatal("earndrop-id is required")
	}

	dbConnPool := connectDBPool()

	earndropSvc := generatesvc.NewService(dbConnPool, *earndropId)

	ctx := context.Background()
	_ = earndropSvc.InitEarndropData(ctx)
}

func connectDBPool() *wpgx.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := rickWpgx.NewWPGXPool(ctx, "postgres")
	if err != nil {
		log.Fatalf("Failed to create database pool: %v", err)
		return nil
	}

	return pool

}

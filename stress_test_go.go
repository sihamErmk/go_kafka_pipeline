package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	internalproducer "go_kafka_pipeline/internalproducer"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: .env file not found: %v", err)
	}

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	fmt.Println("💥💥💥 MAXIMUM STRESS TEST 💥💥💥")
	fmt.Println("50 producers × 1M messages = 50M total")
	fmt.Println("This WILL stress your system!")

	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	if brokers[0] == "" {
		brokers = []string{"localhost:9092"}
	}
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "seed-status-updates"
	}

	startTime := time.Now()
	
	var wg sync.WaitGroup
	errorChan := make(chan error, 50)
	successChan := make(chan int, 50)

	for i := 1; i <= 50; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			
			testProducer, err := internalproducer.NewTestProducer(brokers, topic, logger)
			if err != nil {
				errorChan <- fmt.Errorf("producer %d failed: %w", producerID, err)
				return
			}
			defer testProducer.Close()

			config := internalproducer.TestDataConfig{
				Brokers:     brokers,
				Topic:       topic,
				RecordCount: 1000000,
				Concurrency: 10,
				BatchSize:   1000,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer cancel()

			if err := testProducer.GenerateTestDataConcurrently(ctx, config); err != nil {
				errorChan <- fmt.Errorf("producer %d failed: %w", producerID, err)
				return
			}

			successChan <- config.RecordCount
			fmt.Printf("[DONE] Producer %d completed\n", producerID)
		}(i)
	}

	fmt.Println("💥 50 MAXIMUM STRESS PRODUCERS STARTED!")

	go func() {
		wg.Wait()
		close(errorChan)
		close(successChan)
	}()

	totalSuccess := 0
	var errors []error

	for {
		select {
		case err, ok := <-errorChan:
			if !ok {
				errorChan = nil
			} else if err != nil {
				errors = append(errors, err)
				fmt.Printf("[ERROR] %v\n", err)
			}
		case count, ok := <-successChan:
			if !ok {
				successChan = nil
			} else {
				totalSuccess += count
			}
		}

		if errorChan == nil && successChan == nil {
			break
		}
	}

	duration := time.Since(startTime)
	throughput := float64(totalSuccess) / duration.Seconds()

	fmt.Println("🏁 MAXIMUM STRESS COMPLETED!")
	fmt.Printf("Total Records: %d\n", totalSuccess)
	fmt.Printf("Duration: %v\n", duration)
	fmt.Printf("Throughput: %.2f records/sec\n", throughput)
	fmt.Printf("Errors: %d\n", len(errors))

	if len(errors) > 0 {
		fmt.Println("\nErrors encountered:")
		for _, err := range errors {
			fmt.Printf("  - %v\n", err)
		}
		os.Exit(1)
	}
}


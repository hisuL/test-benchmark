package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/sirupsen/logrus"

	"test-benchmark/internal/config"
	"test-benchmark/internal/database"
	"test-benchmark/internal/generator"
	"test-benchmark/internal/models"
)

type Benchmark struct {
	config       *config.Config
	logger       *logrus.Logger
	databases    []database.Database
	testData     []models.SensorData
	jobCount     int
	rand         *rand.Rand
	deviceCount  int
	factoryCount int
}

func NewBenchmark(cfg *config.Config, logger *logrus.Logger) *Benchmark {
	return &Benchmark{
		config:       cfg,
		logger:       logger,
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
		deviceCount:  cfg.DataGeneration.DeviceCount,
		factoryCount: cfg.DataGeneration.FactoryCount,
	}
}

func (b *Benchmark) AddDatabase(db database.Database) {
	b.databases = append(b.databases, db)
}

func (b *Benchmark) GenerateTestData() error {
	b.logger.Info("Generating test data...")

	gen := generator.NewDataGenerator(&b.config.DataGeneration)
	data, err := gen.GenerateData()
	if err != nil {
		return fmt.Errorf("failed to generate test data: %w", err)
	}

	b.testData = data
	b.logger.Infof("Generated %d test records", len(data))
	b.jobCount = b.config.DataGeneration.JobCount

	return nil
}

func (b *Benchmark) RunWriteBenchmark(ctx context.Context) ([]models.WriteResult, error) {
	b.logger.Info("Starting write benchmark...")

	var results []models.WriteResult

	for _, db := range b.databases {
		b.logger.Infof("Testing write performance for %s", db.Name())

		// Connect and prepare schema
		if err := db.Connect(ctx); err != nil {
			b.logger.Errorf("Failed to connect to %s: %v", db.Name(), err)
			continue
		}

		if err := db.DropSchema(ctx); err != nil {
			b.logger.Warnf("Failed to drop schema for %s: %v", db.Name(), err)
		}

		if err := db.CreateSchema(ctx); err != nil {
			b.logger.Errorf("Failed to create schema for %s: %v", db.Name(), err)
			db.Close()
			continue
		}

		// Measure write performance
		result, err := b.measureWritePerformance(ctx, db)
		if err != nil {
			b.logger.Errorf("Write benchmark failed for %s: %v", db.Name(), err)
			db.Close()
			continue
		}

		results = append(results, result)

		b.logger.Infof("%s write results: %.2f records/sec, avg latency: %v",
			db.Name(), result.Throughput, result.AvgLatency)
	}

	return results, nil
}

func (b *Benchmark) measureWritePerformance(ctx context.Context, db database.Database) (models.WriteResult, error) {
	batchSize := b.config.DataGeneration.BatchSize
	concurrency := b.config.Benchmark.ConcurrencyLevels[0] // 假设从配置中获取并发数，你也可以设为固定值

	var latenciesMutex sync.Mutex
	var latencies []float64
	var errorCount int64

	startTime := time.Now()

	// 创建批次任务队列 - 直接使用 []models.SensorData
	batches := make([][]models.SensorData, 0)
	for i := 0; i < len(b.testData); i += batchSize {
		end := i + batchSize
		if end > len(b.testData) {
			end = len(b.testData)
		}
		batches = append(batches, b.testData[i:end])
	}

	// 创建任务通道和等待组
	batchChan := make(chan []models.SensorData, len(batches))
	var wg sync.WaitGroup

	// 将所有批次放入通道
	for _, batch := range batches {
		batchChan <- batch
	}
	close(batchChan)

	// 启动 goroutine 处理批次
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for batch := range batchChan {
				batchStart := time.Now()
				err := db.WriteBatch(ctx, batch)
				batchDuration := time.Since(batchStart)

				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					b.logger.Errorf("Batch write error: %v", err)
				} else {
					latencyMs := float64(batchDuration.Nanoseconds()) / 1e6 // Convert to milliseconds

					// 线程安全地添加延迟数据
					latenciesMutex.Lock()
					latencies = append(latencies, latencyMs)
					latenciesMutex.Unlock()
				}
			}
		}()
	}

	// 等待所有 goroutine 完成
	wg.Wait()

	totalDuration := time.Since(startTime)

	// 计算统计信息
	var avgLatency, p95Latency, p99Latency float64
	if len(latencies) > 0 {
		avgLatency, _ = stats.Mean(latencies)
		p95Latency, _ = stats.Percentile(latencies, 95)
		p99Latency, _ = stats.Percentile(latencies, 99)
	}

	throughput := float64(len(b.testData)) / totalDuration.Seconds()

	return models.WriteResult{
		Database:     db.Name(),
		TotalRecords: int64(len(b.testData)),
		JobCount:     b.jobCount,
		Duration:     totalDuration,
		Throughput:   throughput,
		AvgLatency:   time.Duration(avgLatency * 1e6), // Convert back to nanoseconds
		P95Latency:   time.Duration(p95Latency * 1e6),
		P99Latency:   time.Duration(p99Latency * 1e6),
		ErrorCount:   errorCount,
	}, nil
}

func (b *Benchmark) RunQueryBenchmark(ctx context.Context) ([]models.QueryResult, error) {
	b.logger.Info("Starting query benchmark...")

	var allResults []models.QueryResult

	queryTypes := []string{"point_query", "aggregation", "range_query", "group_by"}

	for _, db := range b.databases {
		b.logger.Infof("Testing query performance for %s", db.Name())

		b.logger.Info("check db connection before running query benchmark")

		// Connect and prepare schema
		if err := db.Connect(ctx); err != nil {
			b.logger.Errorf("Failed to connect to %s: %v", db.Name(), err)
			continue
		}

		for _, concurrency := range b.config.Benchmark.ConcurrencyLevels {
			b.logger.Infof("Testing with %d concurrent connections", concurrency)

			for _, queryType := range queryTypes {
				result, err := b.measureQueryPerformance(ctx, db, queryType, concurrency)
				if err != nil {
					b.logger.Errorf("Query benchmark failed for %s (type: %s, concurrency: %d): %v",
						db.Name(), queryType, concurrency, err)
					continue
				}

				allResults = append(allResults, result)

				b.logger.Infof("%s %s (concurrency %d): %.2f QPS, avg latency: %v",
					db.Name(), queryType, concurrency, result.QPS, result.AvgLatency)
			}
		}
	}

	return allResults, nil
}

func (b *Benchmark) measureQueryPerformance(ctx context.Context, db database.Database, queryType string, concurrency int) (models.QueryResult, error) {
	duration := time.Duration(b.config.Benchmark.QueryDurationSeconds) * time.Second
	warmup := time.Duration(b.config.Benchmark.WarmupDurationSeconds) * time.Second

	var totalQueries int64
	var totalErrors int64
	var latencies []float64
	var latencyMutex sync.Mutex

	// Create worker pool
	var wg sync.WaitGroup
	queryCtx, cancel := context.WithTimeout(ctx, duration+warmup+10*time.Second)
	defer cancel()

	// Warmup phase
	b.logger.Infof("Warmup phase for %s", queryType)
	warmupEnd := time.Now().Add(warmup)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(warmupEnd) {
				b.executeQuery(queryCtx, db, queryType, false, nil, nil, nil)
				time.Sleep(10 * time.Millisecond) // Small delay between queries
			}
		}()
	}
	wg.Wait()

	// Actual benchmark phase
	b.logger.Infof("Benchmark phase for %s", queryType)
	benchmarkEnd := time.Now().Add(duration)
	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localLatencies := make([]float64, 0, 1000)

			for time.Now().Before(benchmarkEnd) {
				queryStart := time.Now()
				err := b.executeQuery(queryCtx, db, queryType, true, &totalQueries, &totalErrors, nil)
				queryDuration := time.Since(queryStart)

				if err == nil {
					localLatencies = append(localLatencies, float64(queryDuration.Nanoseconds())/1e6)
				}

				time.Sleep(10 * time.Millisecond)
			}

			// Merge local latencies
			latencyMutex.Lock()
			latencies = append(latencies, localLatencies...)
			latencyMutex.Unlock()
		}()
	}
	wg.Wait()

	actualDuration := time.Since(startTime)

	// Calculate statistics
	if len(latencies) == 0 {
		return models.QueryResult{}, fmt.Errorf("no successful queries executed")
	}

	sort.Float64s(latencies)
	avgLatency, _ := stats.Mean(latencies)
	p95Latency, _ := stats.Percentile(latencies, 95)
	p99Latency, _ := stats.Percentile(latencies, 99)

	qps := float64(totalQueries) / actualDuration.Seconds()
	successRate := float64(totalQueries-totalErrors) / float64(totalQueries) * 100

	return models.QueryResult{
		Database:     db.Name(),
		QueryType:    queryType,
		Concurrency:  concurrency,
		Duration:     actualDuration,
		TotalQueries: totalQueries,
		QPS:          qps,
		AvgLatency:   time.Duration(avgLatency * 1e6),
		P95Latency:   time.Duration(p95Latency * 1e6),
		P99Latency:   time.Duration(p99Latency * 1e6),
		ErrorCount:   totalErrors,
		SuccessRate:  successRate,
	}, nil
}

func (b *Benchmark) executeQuery(ctx context.Context, db database.Database, queryType string, countMetrics bool, totalQueries, totalErrors *int64, latencies *[]float32) error {
	if countMetrics {
		atomic.AddInt64(totalQueries, 1)
	}

	if err := db.Connect(ctx); err != nil {
		b.logger.Errorf("Failed to connect to %s: %v", db.Name(), err)
	}

	var err error

	startDay := b.config.DataGeneration.Day
	start, _ := generator.ParseDay(startDay)
	end := start.Add(24 * time.Hour)
	allJobIndex := b.config.DataGeneration.JobCount
	//从0到allJobIndex-1生成一个随机的jobId
	randomId := b.rand.Intn(allJobIndex) + 1
	jobId := fmt.Sprintf("job_%06d", randomId)
	factoryId, deviceId := generator.GetDeviceByJobId(randomId, b.factoryCount, b.deviceCount)
	fmt.Println("Executing query for jobId:", jobId, "factoryId:", factoryId, "deviceId:", deviceId)
	deviceIdStr := fmt.Sprintf("device_%03d", deviceId)
	switch queryType {
	case "point_query":
		_, err = db.QueryByDeviceAndTimeRange(ctx, jobId, deviceIdStr, start, end)

	case "aggregation":
		_, err = db.QueryAggregation(ctx, jobId, deviceIdStr, start, end, "avg")

	case "range_query":
		_, err = db.QueryTimeRange(ctx, jobId, start, end, 1000)

	case "group_by":
		_, err = db.QueryGroupBy(ctx, jobId, start, end, "device_id", 1*time.Hour)

	default:
		err = fmt.Errorf("unknown query type: %s", queryType)
	}

	if err != nil && countMetrics {
		atomic.AddInt64(totalErrors, 1)
	}

	return err
}

func (b *Benchmark) GenerateReport(writeResults []models.WriteResult, queryResults []models.QueryResult) *models.BenchmarkReport {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	report := &models.BenchmarkReport{
		Timestamp:    time.Now(),
		WriteResults: writeResults,
		QueryResults: queryResults,
		SystemInfo: models.SystemInfo{
			CPUCores: runtime.NumCPU(),
			Memory:   fmt.Sprintf("%.2f GB", float32(m.Sys)/1024/1024/1024),
			OS:       runtime.GOOS,
		},
	}

	return report
}

func (b *Benchmark) PrintResults(report *models.BenchmarkReport) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("TSDB BENCHMARK REPORT")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Generated at: %s\n", report.Timestamp.Format("2006-01-02 15:04:05"))
	fmt.Printf("System: %s, CPU Cores: %d, Memory: %s\n\n",
		report.SystemInfo.OS, report.SystemInfo.CPUCores, report.SystemInfo.Memory)

	// Write performance results
	fmt.Println("WRITE PERFORMANCE")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("%-12s %-12s %-12s %-12s %-12s %-12s %-12s\n",
		"Database", "Records", "jobCount", "Duration", "Throughput", "Avg Latency", "P99 Latency")
	fmt.Println(strings.Repeat("-", 80))

	for _, result := range report.WriteResults {
		fmt.Printf("%-12s %-12d %-12d %-12s %-12.0f %-12s %-12s\n",
			result.Database,
			result.TotalRecords,
			result.JobCount,
			result.Duration.Round(time.Second),
			result.Throughput,
			result.AvgLatency.Round(time.Millisecond),
			result.P99Latency.Round(time.Millisecond))
	}

	// Query performance results by concurrency
	fmt.Println("\nQUERY PERFORMANCE BY CONCURRENCY")
	fmt.Println(strings.Repeat("-", 120))

	for _, concurrency := range b.config.Benchmark.ConcurrencyLevels {
		fmt.Printf("\nConcurrency Level: %d\n", concurrency)
		fmt.Println(strings.Repeat("-", 120))
		fmt.Printf("%-12s %-15s %-8s %-12s %-12s %-12s %-12s\n",
			"Database", "Query Type", "QPS", "Avg Latency", "P95 Latency", "P99 Latency", "Success Rate")
		fmt.Println(strings.Repeat("-", 120))

		for _, result := range report.QueryResults {
			if result.Concurrency == concurrency {
				fmt.Printf("%-12s %-15s %-8.1f %-12s %-12s %-12s %-12.1f%%\n",
					result.Database,
					result.QueryType,
					result.QPS,
					result.AvgLatency.Round(time.Millisecond),
					result.P95Latency.Round(time.Millisecond),
					result.P99Latency.Round(time.Millisecond),
					result.SuccessRate)
			}
		}
	}

	// Summary and recommendations
	b.printSummaryAndRecommendations(report)
}

func (b *Benchmark) printSummaryAndRecommendations(report *models.BenchmarkReport) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("SUMMARY AND RECOMMENDATIONS")
	fmt.Println(strings.Repeat("=", 80))

	// Find best write performance
	var bestWrite models.WriteResult
	for _, result := range report.WriteResults {
		if result.Throughput > bestWrite.Throughput {
			bestWrite = result
		}
	}

	if bestWrite.Database != "" {
		fmt.Printf("Best Write Performance: %s (%.0f records/sec)\n",
			bestWrite.Database, bestWrite.Throughput)
	}

	// Find best query performance by concurrency level
	for _, concurrency := range b.config.Benchmark.ConcurrencyLevels {
		dbQPS := make(map[string]float64)

		for _, result := range report.QueryResults {
			if result.Concurrency == concurrency {
				dbQPS[result.Database] += result.QPS
			}
		}

		var bestDB string
		var bestQPS float64
		for db, qps := range dbQPS {
			if qps > bestQPS {
				bestDB = db
				bestQPS = qps
			}
		}

		if bestDB != "" {
			fmt.Printf("Best Query Performance (Concurrency %d): %s (%.1f total QPS)\n",
				concurrency, bestDB, bestQPS)
		}
	}

	// Recommendations
	fmt.Println("\nRECOMMENDATIONS:")

	// Write performance recommendation
	if len(report.WriteResults) > 0 {
		fmt.Printf("• For high-throughput writes: %s shows best performance\n", bestWrite.Database)

		if bestWrite.Throughput < 10000 {
			fmt.Println("• Consider optimizing batch sizes and parallel writers for better throughput")
		}
	}

	// Query performance recommendation
	lowConcurrencyBest := b.findBestQueryDB(report.QueryResults, []int{1, 5})
	highConcurrencyBest := b.findBestQueryDB(report.QueryResults, []int{50, 100})

	if lowConcurrencyBest != "" {
		fmt.Printf("• For low concurrency queries: %s performs best\n", lowConcurrencyBest)
	}

	if highConcurrencyBest != "" {
		fmt.Printf("• For high concurrency queries: %s performs best\n", highConcurrencyBest)
	}

	fmt.Println("• Consider connection pooling and query optimization for production use")
	fmt.Println("• Monitor memory usage and disk I/O in production environments")
}

func (b *Benchmark) findBestQueryDB(results []models.QueryResult, concurrencyLevels []int) string {
	dbScores := make(map[string]float64)

	for _, result := range results {
		for _, level := range concurrencyLevels {
			if result.Concurrency == level {
				dbScores[result.Database] += result.QPS
			}
		}
	}

	var bestDB string
	var bestScore float64
	for db, score := range dbScores {
		if score > bestScore {
			bestDB = db
			bestScore = score
		}
	}

	return bestDB
}

func (b *Benchmark) Cleanup() {
	for _, db := range b.databases {
		db.Close()
	}
}

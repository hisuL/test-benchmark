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

// 添加一个带时间戳的打印函数
func (b *Benchmark) logf(format string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	fmt.Printf("[%s] %s\n", timestamp, fmt.Sprintf(format, args...))
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

// processBatchesFromChannel 从 channel 中读取数据批次并写入数据库
func (b *Benchmark) processBatchesFromChannel(ctx context.Context, db database.Database, dataChan <-chan []models.SensorData) (models.WriteResult, error) {
	var totalRecords int64
	var totalDuration time.Duration
	var latencies []float64
	var wg sync.WaitGroup
	start := time.Now()

	// 使用 WaitGroup 来等待所有批次处理完成
	wg.Add(1)
	go func() {
		defer wg.Done()
		for batch := range dataChan {
			// 检查上下文是否被取消
			select {
			case <-ctx.Done():
				b.logger.Warnf("上下文取消，停止处理批次")
				return
			default:
			}

			batchStart := time.Now()
			if err := db.WriteBatch(ctx, batch); err != nil {
				// 在实际应用中，这里可能需要更复杂的错误处理
				b.logf("写入批次数据失败 for %s: %v", db.Name(), err)
				continue // 继续处理下一个批次
			}
			batchDuration := time.Since(batchStart)

			totalDuration += batchDuration
			latencies = append(latencies, float64(batchDuration.Microseconds()))
			atomic.AddInt64(&totalRecords, int64(len(batch)))

			// 打印进度
			processed := atomic.LoadInt64(&totalRecords)
			if processed%100000 == 0 {
				b.logf("已为 %s 处理 %d 条记录...", db.Name(), processed)
			}
		}
	}()

	wg.Wait() // 等待 channel 关闭且所有数据处理完毕

	// 检查是否处理了任何记录
	if totalRecords == 0 {
		b.logger.Warnf("没有为 %s 处理任何记录", db.Name())
		return models.WriteResult{Database: db.Name()}, nil
	}

	duration := time.Since(start)
	avgLatency := totalDuration / time.Duration(totalRecords)
	throughput := float64(totalRecords) / duration.Seconds()

	p95, _ := stats.Percentile(latencies, 95)
	p99, _ := stats.Percentile(latencies, 99)

	return models.WriteResult{
		Database:     db.Name(),
		TotalRecords: totalRecords,
		Duration:     duration,
		Throughput:   throughput,
		AvgLatency:   avgLatency,
		P95Latency:   time.Duration(p95) * time.Microsecond,
		P99Latency:   time.Duration(p99) * time.Microsecond,
		JobCount:     b.config.DataGeneration.JobCount,
	}, nil
}

func (b *Benchmark) processBatchesSequentially(ctx context.Context, db database.Database, batchSize int) (models.WriteResult, error) {
	var totalRecords int64
	var totalDuration time.Duration
	var latencies []float64

	// 创建数据生成器
	gen := generator.NewDataGenerator(&b.config.DataGeneration)

	start := time.Now()
	offset := 0
	batchCount := 0

	for {
		// 检查上下文是否被取消
		select {
		case <-ctx.Done():
			b.logger.Warnf("上下文取消，停止处理")
			break
		default:
		}

		// 生成一批数据
		b.logf("生成第 %d 批数据 (从记录 %d 开始)...", batchCount+1, offset+1)
		batchData, isComplete, err := gen.GenerateDataBatch(ctx, offset, batchSize)
		if err != nil {
			return models.WriteResult{Database: db.Name()}, fmt.Errorf("生成数据失败: %v", err)
		}

		if len(batchData) == 0 {
			b.logf("没有更多数据需要处理")
			break
		}

		// 将数据分成更小的批次写入数据库（避免单次写入过多数据）
		const dbWriteBatchSize = 200000 // 20万条记录一次写入
		err = b.writeDataInSmallBatches(ctx, db, batchData, dbWriteBatchSize, &totalRecords, &totalDuration, &latencies)
		if err != nil {
			return models.WriteResult{Database: db.Name()}, fmt.Errorf("写入数据失败: %v", err)
		}

		batchCount++
		offset += len(batchData)

		// 手动释放内存
		batchData = nil
		runtime.GC()

		b.logf("第 %d 批数据处理完成，累计处理 %d 条记录", batchCount, totalRecords)

		if isComplete {
			b.logf("所有数据处理完成")
			break
		}
	}

	if totalRecords == 0 {
		b.logf("没有为 %s 处理任何记录", db.Name())
		return models.WriteResult{Database: db.Name()}, nil
	}

	duration := time.Since(start)
	avgLatency := totalDuration / time.Duration(totalRecords)
	throughput := float64(totalRecords) / duration.Seconds()

	p95, _ := stats.Percentile(latencies, 95)
	p99, _ := stats.Percentile(latencies, 99)

	return models.WriteResult{
		Database:     db.Name(),
		TotalRecords: totalRecords,
		Duration:     duration,
		Throughput:   throughput,
		AvgLatency:   avgLatency,
		P95Latency:   time.Duration(p95) * time.Microsecond,
		P99Latency:   time.Duration(p99) * time.Microsecond,
		JobCount:     b.config.DataGeneration.JobCount,
	}, nil
}

func (b *Benchmark) writeDataInSmallBatches(ctx context.Context, db database.Database, data []models.SensorData,
	writeBatchSize int, totalRecords *int64, totalDuration *time.Duration, latencies *[]float64) error {

	dataLen := len(data)
	b.logf("开始为 %s 写入 %d 条记录，每批次大小为 %d 条记录", db.Name(), dataLen, writeBatchSize)

	for i := 0; i < dataLen; i += writeBatchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		end := i + writeBatchSize
		if end > dataLen {
			end = dataLen
		}

		batch := data[i:end]

		b.logf("为 %s 准备写入批次: %d 到 %d (共 %d 条记录)", db.Name(), i+1, end, len(batch))
		// 写入数据库
		batchStart := time.Now()
		if err := db.WriteBatch(ctx, batch); err != nil {
			return fmt.Errorf("写入批次数据失败: %v", err)
		}
		batchDuration := time.Since(batchStart)

		b.logf("为 %s 写入批次完成，耗时 %v", db.Name(), batchDuration)
		// 更新统计信息
		*totalDuration += batchDuration
		*latencies = append(*latencies, float64(batchDuration.Microseconds()))
		atomic.AddInt64(totalRecords, int64(len(batch)))

		// 打印进度
		processed := atomic.LoadInt64(totalRecords)
		if processed%1000000 == 0 { // 每100万条记录打印一次
			b.logf("已为 %s 处理 %d 条记录...", db.Name(), processed)
		}
	}

	return nil
}

func (b *Benchmark) RunWriteBenchmark(ctx context.Context) ([]models.WriteResult, error) {
	b.logf("Starting write benchmark...")

	var results []models.WriteResult
	const writeBatchSize = 20000000 // 2000万条记录

	for _, db := range b.databases {
		b.logf("Testing write performance for %s", db.Name())

		// Connect and prepare schema
		if err := db.Connect(ctx); err != nil {
			b.logf("Failed to connect to %s: %v", db.Name(), err)
			continue
		}
		defer db.Close()

		if err := db.DropSchema(ctx); err != nil {
			b.logf("Failed to drop schema for %s: %v", db.Name(), err)
		}

		if err := db.CreateSchema(ctx); err != nil {
			b.logf("Failed to create schema for %s: %v", db.Name(), err)
			continue
		}

		// 使用新的同步处理方法
		result, err := b.processBatchesSequentially(ctx, db, writeBatchSize)
		if err != nil {
			b.logf("Write benchmark failed for %s: %v", db.Name(), err)
			continue
		}

		results = append(results, result)

		b.logf("%s write results: %.2f records/sec, avg latency: %v",
			db.Name(), result.Throughput, result.AvgLatency)
	}

	return results, nil
}

func (b *Benchmark) RunQueryBenchmark(ctx context.Context) ([]models.QueryResult, error) {
	b.logger.Info("Starting query benchmark...")

	var allResults []models.QueryResult

	queryTypes := []string{"point_query", "aggregation", "range_query", "group_by"}

	for _, db := range b.databases {
		b.logf("Testing query performance for %s", db.Name())

		b.logf("check db connection before running query benchmark")

		// Connect and prepare schema
		if err := db.Connect(ctx); err != nil {
			b.logger.Errorf("Failed to connect to %s: %v", db.Name(), err)
			continue
		}

		for _, concurrency := range b.config.Benchmark.ConcurrencyLevels {
			b.logf("Testing with %d concurrent connections", concurrency)

			for _, queryType := range queryTypes {
				result, err := b.measureQueryPerformance(ctx, db, queryType, concurrency)
				if err != nil {
					b.logger.Errorf("Query benchmark failed for %s (type: %s, concurrency: %d): %v",
						db.Name(), queryType, concurrency, err)
					continue
				}

				allResults = append(allResults, result)

				b.logf("%s %s (concurrency %d): %.2f QPS, avg latency: %v",
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
	b.logf("Warmup phase for %s", queryType)
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
	b.logf("Benchmark phase for %s", queryType)
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
	b.logf("Executing query for jobId: %s, factoryId: %s, deviceId: %s", jobId, factoryId, deviceId)
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
	b.logf("\n" + strings.Repeat("=", 80))
	b.logf("TSDB BENCHMARK REPORT")
	b.logf(strings.Repeat("=", 80))
	b.logf("Generated at: %s", report.Timestamp.Format("2006-01-02 15:04:05"))
	b.logf("System: %s, CPU Cores: %d, Memory: %s\n",
		report.SystemInfo.OS, report.SystemInfo.CPUCores, report.SystemInfo.Memory)

	// Write performance results
	b.logf("WRITE PERFORMANCE")
	b.logf(strings.Repeat("-", 80))
	b.logf("%-12s %-12s %-12s %-12s %-12s %-12s %-12s",
		"Database", "Records", "jobCount", "Duration", "Throughput", "Avg Latency", "P99 Latency")
	b.logf(strings.Repeat("-", 80))

	for _, result := range report.WriteResults {
		b.logf("%-12s %-12d %-12d %-12s %-12.0f %-12s %-12s",
			result.Database,
			result.TotalRecords,
			result.JobCount,
			result.Duration.Round(time.Second),
			result.Throughput,
			result.AvgLatency.Round(time.Millisecond),
			result.P99Latency.Round(time.Millisecond))
	}

	// Query performance results by concurrency
	b.logf("\nQUERY PERFORMANCE BY CONCURRENCY")
	b.logf(strings.Repeat("-", 120))

	for _, concurrency := range b.config.Benchmark.ConcurrencyLevels {
		b.logf("\nConcurrency Level: %d", concurrency)
		b.logf(strings.Repeat("-", 120))
		b.logf("%-12s %-15s %-8s %-12s %-12s %-12s %-12s",
			"Database", "Query Type", "QPS", "Avg Latency", "P95 Latency", "P99 Latency", "Success Rate")
		b.logf(strings.Repeat("-", 120))

		for _, result := range report.QueryResults {
			if result.Concurrency == concurrency {
				b.logf("%-12s %-15s %-8.1f %-12s %-12s %-12s %-12.1f%%",
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
	b.logf("\n" + strings.Repeat("=", 80))
	b.logf("SUMMARY AND RECOMMENDATIONS")
	b.logf(strings.Repeat("=", 80))

	// Find best write performance
	var bestWrite models.WriteResult
	for _, result := range report.WriteResults {
		if result.Throughput > bestWrite.Throughput {
			bestWrite = result
		}
	}

	if bestWrite.Database != "" {
		b.logf("Best Write Performance: %s (%.0f records/sec)",
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
			b.logf("Best Query Performance (Concurrency %d): %s (%.1f total QPS)",
				concurrency, bestDB, bestQPS)
		}
	}

	// Recommendations
	b.logf("\nRECOMMENDATIONS:")

	// Write performance recommendation
	if len(report.WriteResults) > 0 {
		b.logf("• For high-throughput writes: %s shows best performance", bestWrite.Database)

		if bestWrite.Throughput < 10000 {
			b.logf("• Consider optimizing batch sizes and parallel writers for better throughput")
		}
	}

	// Query performance recommendation
	lowConcurrencyBest := b.findBestQueryDB(report.QueryResults, []int{1, 5})
	highConcurrencyBest := b.findBestQueryDB(report.QueryResults, []int{50, 100})

	if lowConcurrencyBest != "" {
		b.logf("• For low concurrency queries: %s performs best", lowConcurrencyBest)
	}

	if highConcurrencyBest != "" {
		b.logf("• For high concurrency queries: %s performs best", highConcurrencyBest)
	}

	b.logf("• Consider connection pooling and query optimization for production use")
	b.logf("• Monitor memory usage and disk I/O in production environments")
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

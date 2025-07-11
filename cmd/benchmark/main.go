package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"test-benchmark/internal/models"
	"time"

	"github.com/sirupsen/logrus"

	"test-benchmark/internal/benchmark"
	"test-benchmark/internal/config"
	"test-benchmark/internal/database"
)

func main() {
	var (
		configPath   = flag.String("config", "configs/config.yaml", "Path to configuration file")
		outputDir    = flag.String("output", "results", "Output directory for results")
		skipWrite    = flag.Bool("skip-write", false, "Skip write benchmark")
		skipQuery    = flag.Bool("skip-query", false, "Skip query benchmark")
		dbFilter     = flag.String("database", "", "Run benchmark for specific database only (influxdb|tdengine|iotdb)")
		skipGenerate = flag.Bool("skip-generate", false, "Skip data generation step, use existing data if available")
	)
	flag.Parse()

	// 添加参数验证日志
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	// 打印参数值，使用与 flag 定义一致的名称
	logger.Infof("Command line parameters: skip-generate=%v, skip-write=%v, skip-query=%v, database=%v",
		*skipGenerate, *skipWrite, *skipQuery, *dbFilter)

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Fatalf("Error loading config: %v", err)
	}

	// Create output directory
	os.MkdirAll(*outputDir, 0755)

	// Setup benchmark
	bench := benchmark.NewBenchmark(cfg, logger)

	// Initialize databases based on filter
	if *dbFilter == "" || *dbFilter == "influxdb" {
		influxDB := database.NewInfluxDB(
			cfg.Databases.InfluxDB.URL,
			cfg.Databases.InfluxDB.Token,
			cfg.Databases.InfluxDB.Org,
			cfg.Databases.InfluxDB.Bucket,
		)
		bench.AddDatabase(influxDB)
	}

	if *dbFilter == "" || *dbFilter == "tdengine" {
		tdConfig := cfg.Databases.TDengine
		tdengine := database.NewTDengineDB(
			tdConfig.Host,
			tdConfig.Port,
			tdConfig.Username,
			tdConfig.Password,
			tdConfig.Database,
		)
		bench.AddDatabase(tdengine)
	}

	if *dbFilter == "" || *dbFilter == "iotdb" {
		iotdb := database.NewIoTDB(
			cfg.Databases.IoTDB.Host,
			cfg.Databases.IoTDB.Port,
			cfg.Databases.IoTDB.Username,
			cfg.Databases.IoTDB.Password,
			"benchmark",
		)
		bench.AddDatabase(iotdb)
	}

	// Generate test data
	if !*skipGenerate {
		logger.Info("Generating test data...")
		err = bench.GenerateTestData()
		if err != nil {
			logger.Fatalf("Error generating test data: %v", err)
		}
	} else {
		logger.Info("skipping data generation step, using existing data if available")
	}

	ctx := context.Background()
	var writeResults []models.WriteResult
	var queryResults []models.QueryResult

	// Run write benchmark
	if !*skipWrite {
		logger.Info("Starting write benchmark...")
		writeResults, err = bench.RunWriteBenchmark(ctx)
		if err != nil {
			logger.Errorf("Write benchmark error: %v", err)
		}
	}

	// Run query benchmark
	if !*skipQuery {
		logger.Info("Starting query benchmark...")
		queryResults, err = bench.RunQueryBenchmark(ctx)
		if err != nil {
			logger.Errorf("Query benchmark error: %v", err)
		}
	}

	// Generate and save report
	report := bench.GenerateReport(writeResults, queryResults)

	// Print results to console
	bench.PrintResults(report)

	// Save detailed results to JSON
	timestamp := time.Now().Format("20060102_150405")
	reportFile := filepath.Join(*outputDir, fmt.Sprintf("benchmark_report_%s.json", timestamp))

	file, err := os.Create(reportFile)
	if err != nil {
		logger.Errorf("Error creating report file: %v", err)
	} else {
		defer file.Close()
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		encoder.Encode(report)
		logger.Infof("Detailed report saved to: %s", reportFile)
	}

	// Save CSV report for easy analysis
	csvFile := filepath.Join(*outputDir, fmt.Sprintf("benchmark_summary_%s.csv", timestamp))
	err = saveCSVReport(report, csvFile)
	if err != nil {
		logger.Errorf("Error creating CSV report: %v", err)
	} else {
		logger.Infof("CSV report saved to: %s", csvFile)
	}

	// Cleanup
	bench.Cleanup()
	logger.Info("Benchmark completed successfully!")
}

func saveCSVReport(report *models.BenchmarkReport, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header
	fmt.Fprintf(file, "Metric Type,Database,Query Type,Concurrency,Value,Unit\n")

	// Write performance data
	for _, result := range report.WriteResults {
		fmt.Fprintf(file, "Write,\"%s\",N/A,1,%.2f,records/sec\n", result.Database, result.Throughput)
		fmt.Fprintf(file, "Write Latency P99,\"%s\",N/A,1,%.2f,ms\n", result.Database, float64(result.P99Latency.Nanoseconds())/1e6)
	}

	for _, result := range report.QueryResults {
		fmt.Fprintf(file, "Query,\"%s\",\"%s\",%d,%.2f,QPS\n",
			result.Database, result.QueryType, result.Concurrency, result.QPS)
		fmt.Fprintf(file, "Query Latency P99,\"%s\",\"%s\",%d,%.2f,ms\n",
			result.Database, result.QueryType, result.Concurrency, float64(result.P99Latency.Nanoseconds())/1e6)
	}

	return nil
}

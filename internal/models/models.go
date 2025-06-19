package models

import (
	"time"
)

type SensorData struct {
	Timestamp       time.Time `json:"timestamp"`
	FactoryID       string    `json:"factory_id"`
	DeviceID        string    `json:"device_id"`
	Temperature     float32   `json:"temperature"`
	Humidity        float32   `json:"humidity"`
	Pressure        float32   `json:"pressure"`
	Voltage         float32   `json:"voltage"`
	Current         float32   `json:"current"`
	Power           float32   `json:"power"`
	RPM             int64     `json:"rpm"`
	Status          string    `json:"status"`
	ErrorCode       int32     `json:"error_code"`
	ProductionCount int64     `json:"production_count"`
}

type WriteResult struct {
	Database     string        `json:"database"`
	TotalRecords int64         `json:"total_records"`
	Duration     time.Duration `json:"duration"`
	Throughput   float64       `json:"throughput"` // records per second
	AvgLatency   time.Duration `json:"avg_latency"`
	P95Latency   time.Duration `json:"p95_latency"`
	P99Latency   time.Duration `json:"p99_latency"`
	ErrorCount   int64         `json:"error_count"`
}

type QueryResult struct {
	Database     string        `json:"database"`
	QueryType    string        `json:"query_type"`
	Concurrency  int           `json:"concurrency"`
	Duration     time.Duration `json:"duration"`
	TotalQueries int64         `json:"total_queries"`
	QPS          float64       `json:"qps"`
	AvgLatency   time.Duration `json:"avg_latency"`
	P95Latency   time.Duration `json:"p95_latency"`
	P99Latency   time.Duration `json:"p99_latency"`
	ErrorCount   int64         `json:"error_count"`
	SuccessRate  float64       `json:"success_rate"`
}

type BenchmarkReport struct {
	Timestamp    time.Time     `json:"timestamp"`
	WriteResults []WriteResult `json:"write_results"`
	QueryResults []QueryResult `json:"query_results"`
	SystemInfo   SystemInfo    `json:"system_info"`
}

type SystemInfo struct {
	CPUCores int    `json:"cpu_cores"`
	Memory   string `json:"memory"`
	OS       string `json:"os"`
}

type QueryRequest struct {
	Type      string                 `json:"type"`
	Params    map[string]interface{} `json:"params"`
	StartTime time.Time              `json:"start_time"`
	EndTime   time.Time              `json:"end_time"`
}

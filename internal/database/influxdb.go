package database

import (
	"context"
	"fmt"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/query"
	"test-benchmark/internal/models"
)

type InfluxDB struct {
	client   influxdb2.Client
	writeAPI api.WriteAPI
	queryAPI api.QueryAPI
	org      string
	bucket   string
}

func NewInfluxDB(url, token, org, bucket string) *InfluxDB {
	client := influxdb2.NewClient(url, token)
	return &InfluxDB{
		client: client,
		org:    org,
		bucket: bucket,
	}
}

func (db *InfluxDB) Name() string {
	return "InfluxDB"
}

func (db *InfluxDB) Connect(ctx context.Context) error {
	//如果 writeAPI 和 queryAPI 已经初始化，则不需要重新初始化
	if db.writeAPI != nil && db.queryAPI != nil {
		return nil
	}
	db.writeAPI = db.client.WriteAPI(db.org, db.bucket)
	db.queryAPI = db.client.QueryAPI(db.org)

	// Test connection
	return db.Ping(ctx)
}

func (db *InfluxDB) Close() error {
	db.writeAPI.Flush()
	db.client.Close()
	return nil
}

func (db *InfluxDB) CreateSchema(ctx context.Context) error {
	// InfluxDB is schemaless, no need to create schema
	return nil
}

func (db *InfluxDB) DropSchema(ctx context.Context) error {
	// Delete all data from bucket
	/*	query := fmt.Sprintf(`
		    from(bucket: "%s")
		    |> range(start: 1970-01-01T00:00:00Z)
		    |> drop()
		`, db.bucket)

			_, err := db.queryAPI.Query(ctx, query)*/
	return nil
}

/*
	func (db *InfluxDB) WriteBatch(ctx context.Context, data []models.SensorData) error {
		for _, record := range data {
			p := influxdb2.NewPointWithMeasurement("sensor_data2").
				AddTag("factory_id", record.FactoryID).
				AddTag("device_id", record.DeviceID).
				AddField("status", record.Status).
				AddTag("job_id", record.JobId).
				AddField("temperature", record.Temperature).
				AddField("humidity", record.Humidity).
				AddField("pressure", record.Pressure).
				AddField("voltage", record.Voltage).
				AddField("current", record.Current).
				AddField("power", record.Power).
				AddField("rpm", record.RPM).
				AddField("error_code", record.ErrorCode).
				AddField("production_count", record.ProductionCount).
				SetTime(record.Timestamp)

			db.writeAPI.WritePoint(p)
		}

		db.writeAPI.Flush()
		return nil
	}
*/
func (db *InfluxDB) WriteBatch(ctx context.Context, data []models.SensorData) error {
	const batchSize = 300
	const workerCount = 10 // 设定同时运行的 worker 数量

	// 创建一个任务通道
	tasks := make(chan []models.SensorData)
	var wg sync.WaitGroup

	// 启动 worker goroutines
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range tasks {
				for _, record := range batch {
					p := influxdb2.NewPointWithMeasurement("sensor_data2").
						AddTag("factory_id", record.FactoryID).
						AddTag("device_id", record.DeviceID).
						AddField("status", record.Status).
						AddTag("job_id", record.JobId).
						AddField("temperature", record.Temperature).
						AddField("humidity", record.Humidity).
						AddField("pressure", record.Pressure).
						AddField("voltage", record.Voltage).
						AddField("current", record.Current).
						AddField("power", record.Power).
						AddField("rpm", record.RPM).
						AddField("error_code", record.ErrorCode).
						AddField("production_count", record.ProductionCount).
						SetTime(record.Timestamp)

					db.writeAPI.WritePoint(p)
				}
			}
		}()
	}

	// 将数据分批并发送到任务通道
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}
		tasks <- data[i:end] // 发送批次到通道
	}

	close(tasks) // 关闭任务通道，表示没有更多任务
	wg.Wait()    // 等待所有 goroutines 完成

	db.writeAPI.Flush() // 确保所有数据都被写入
	return nil
}

func (db *InfluxDB) QueryByDeviceAndTimeRange(ctx context.Context, jobId string, deviceID string, start, end time.Time) ([]models.SensorData, error) {
	queryStr := fmt.Sprintf(`
        from(bucket: "%s")
        |> range(start: %s, stop: %s)
        |> filter(fn: (r) => r._measurement == "sensor_data2")
		|> filter(fn: (r) => r["_field"] == "temperature")
        |> filter(fn: (r) => r.device_id == "%s")
		|> filter(fn: (r) => r.job_id == "%s")
		|> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
		|> yield(name: "mean")
    `, db.bucket, start.Format(time.RFC3339), end.Format(time.RFC3339), deviceID, jobId)
	fmt.Printf(queryStr)

	result, err := db.queryAPI.Query(ctx, queryStr)
	if err != nil {
		fmt.Printf("QueryByDeviceAndTimeRange error: %v", err)
		return nil, err
	}

	var data []models.SensorData
	dateSize := 0
	for result.Next() {
		dateSize++
		record := result.Record()
		sensorData := models.SensorData{
			Timestamp: record.Time(),
		}
		data = append(data, sensorData)
	}
	if dateSize > 0 {
		fmt.Printf("QueryByDeviceAndTimeRange Data size: %d\n", dateSize)
	}
	fmt.Printf("data size: %d\n", dateSize)
	return data, result.Err()
}

func (db *InfluxDB) QueryAggregation(ctx context.Context, jobId string, deviceID string, start, end time.Time, aggType string) (float32, error) {
	var aggFunc string
	switch aggType {
	case "avg":
		aggFunc = "mean"
	case "max":
		aggFunc = "max"
	case "min":
		aggFunc = "min"
	default:
		aggFunc = "mean"
	}

	queryStr := fmt.Sprintf(`
        from(bucket: "%s")
        |> range(start: %s, stop: %s)
        |> filter(fn: (r) => r._measurement == "sensor_data2")
        |> filter(fn: (r) => r.job_id == "%s")
        |> filter(fn: (r) => r.device_id == "%s")
        |> filter(fn: (r) => r._field == "temperature")
        |> aggregateWindow(every: 1m, fn: %s, createEmpty: false)
        |> yield(name: "minute_aggregation")
    `, db.bucket, start.Format(time.RFC3339), end.Format(time.RFC3339), jobId, deviceID, aggFunc)

	fmt.Printf("Query: %s\n", queryStr)

	result, err := db.queryAPI.Query(ctx, queryStr)
	if err != nil {
		return 0, err
	}
	defer result.Close()

	dataSize := 0
	for result.Next() {
		dataSize += 1
	}

	fmt.Printf("QueryAggregation Data size: %d\n", dataSize)
	return 0, nil
}

func (db *InfluxDB) QueryTimeRange(ctx context.Context, jobId string, start, end time.Time, limit int) ([]models.SensorData, error) {
	queryStr := fmt.Sprintf(`
        from(bucket: "%s")
        |> range(start: %s, stop: %s)
        |> filter(fn: (r) => r._measurement == "sensor_data2")
        |> filter(fn: (r) => r.job_id == "%s")
        |> filter(fn: (r) => r._field == "temperature")
        |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
        |> limit(n: %d)
        |> yield(name: "minute_aggregation")
    `, db.bucket, start.Format(time.RFC3339), end.Format(time.RFC3339), jobId, limit)

	fmt.Printf(queryStr)
	result, err := db.queryAPI.Query(ctx, queryStr)
	if err != nil {
		return nil, err
	}

	var data []models.SensorData
	dataSize := 0
	for result.Next() {
		record := result.Record()
		dataSize += 1
		sensorData := models.SensorData{
			Timestamp:   record.Time(),
			FactoryID:   getStringValue(record, "factory_id"),
			DeviceID:    getStringValue(record, "device_id"),
			Temperature: getFloatValue(record, "temperature"),
		}
		data = append(data, sensorData)
	}

	fmt.Printf("QueryTimeRange Data size: %d\n", dataSize)
	return data, result.Err()
}

func (db *InfluxDB) QueryGroupBy(ctx context.Context, jobId string, start, end time.Time, groupBy string, interval time.Duration) (map[string]float32, error) {
	var groupByClause string
	switch groupBy {
	case "device_id":
		groupByClause = `|> group(columns: ["device_id"])`
	case "factory":
		groupByClause = `|> group(columns: ["factory_id"])`
	default:
		groupByClause = fmt.Sprintf(`|> aggregateWindow(every: 1m, fn: mean)`)
	}

	queryStr := fmt.Sprintf(`
        from(bucket: "%s")
        |> range(start: %s, stop: %s)
        |> filter(fn: (r) => r._measurement == "sensor_data2")
        |> filter(fn: (r) => r._field == "temperature")
		|> filter(fn: (r) => r.job_id == "%s")
	    %s
  		|> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
    `, db.bucket, start.Format(time.RFC3339), end.Format(time.RFC3339), jobId, groupByClause)
	fmt.Printf(queryStr)
	result, err := db.queryAPI.Query(ctx, queryStr)
	if err != nil {
		return nil, err
	}

	data := make(map[string]float32)
	dataSize := 0
	for result.Next() {
		record := result.Record()
		key := fmt.Sprintf("%v", record.ValueByKey(groupBy+"_id"))
		if key == "<nil>" {
			key = record.Time().Format(time.RFC3339)
		}
		data[key] = getFloatValue(record, "_value")
		dataSize += 1
	}

	fmt.Printf("QueryGroupBy Data size: %d\n", dataSize)
	return data, result.Err()
}

func (db *InfluxDB) Ping(ctx context.Context) error {
	health, err := db.client.Health(ctx)
	if err != nil {
		return err
	}

	if health.Status != "pass" {
		return fmt.Errorf("influxdb health check failed: %s", health.Status)
	}

	return nil
}

// Helper functions
func getStringValue(record *query.FluxRecord, key string) string {
	val := record.ValueByKey(key)
	if val == nil {
		return ""
	}
	return fmt.Sprintf("%v", val)
}

func getFloatValue(record *query.FluxRecord, key string) float32 {
	val := record.ValueByKey(key)
	if val == nil {
		return 0
	}
	if f, ok := val.(float64); ok {
		return float32(f)
	}
	return 0
}

func getIntValue(record *query.FluxRecord, key string) int64 {
	val := record.ValueByKey(key)
	if val == nil {
		return 0
	}
	if i, ok := val.(int64); ok {
		return i
	}
	return 0
}

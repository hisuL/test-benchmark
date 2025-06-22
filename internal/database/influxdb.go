package database

import (
	"context"
	"fmt"
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
	query := fmt.Sprintf(`
        from(bucket: "%s")
        |> range(start: 1970-01-01T00:00:00Z)
        |> drop()
    `, db.bucket)

	_, err := db.queryAPI.Query(ctx, query)
	return err
}

func (db *InfluxDB) WriteBatch(ctx context.Context, data []models.SensorData) error {
	for _, record := range data {
		p := influxdb2.NewPointWithMeasurement("sensor_data").
			AddTag("factory_id", record.FactoryID).
			AddTag("device_id", record.DeviceID).
			AddTag("status", record.Status).
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

func (db *InfluxDB) QueryByDeviceAndTimeRange(ctx context.Context, deviceID string, start, end time.Time) ([]models.SensorData, error) {
	queryStr := fmt.Sprintf(`
        from(bucket: "%s")
        |> range(start: %s, stop: %s)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r.device_id == "%s")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    `, db.bucket, start.Format(time.RFC3339), end.Format(time.RFC3339), deviceID)
	fmt.Printf(queryStr)

	result, err := db.queryAPI.Query(ctx, queryStr)
	if err != nil {
		return nil, err
	}

	var data []models.SensorData
	for result.Next() {
		record := result.Record()
		sensorData := models.SensorData{
			Timestamp:       record.Time(),
			FactoryID:       getStringValue(record, "factory_id"),
			DeviceID:        getStringValue(record, "device_id"),
			Temperature:     getFloatValue(record, "temperature"),
			Humidity:        getFloatValue(record, "humidity"),
			Pressure:        getFloatValue(record, "pressure"),
			Voltage:         getFloatValue(record, "voltage"),
			Current:         getFloatValue(record, "current"),
			Power:           getFloatValue(record, "power"),
			RPM:             getIntValue(record, "rpm"),
			Status:          getStringValue(record, "status"),
			ErrorCode:       int32(getIntValue(record, "error_code")),
			ProductionCount: getIntValue(record, "production_count"),
		}
		data = append(data, sensorData)
	}

	return data, result.Err()
}

func (db *InfluxDB) QueryAggregation(ctx context.Context, deviceID string, start, end time.Time, aggType string) (float32, error) {
	var aggFunc string
	switch aggType {
	case "avg":
		aggFunc = "mean()"
	case "max":
		aggFunc = "max()"
	case "min":
		aggFunc = "min()"
	default:
		aggFunc = "mean()"
	}

	queryStr := fmt.Sprintf(`
        from(bucket: "%s")
        |> range(start: %s, stop: %s)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r.device_id == "%s")
        |> filter(fn: (r) => r._field == "temperature")
        |> %s
    `, db.bucket, start.Format(time.RFC3339), end.Format(time.RFC3339), deviceID, aggFunc)
	fmt.Printf(queryStr)
	result, err := db.queryAPI.Query(ctx, queryStr)
	if err != nil {
		return 0, err
	}

	if result.Next() {
		return getFloatValue(result.Record(), "_value"), nil
	}

	return 0, result.Err()
}

func (db *InfluxDB) QueryTimeRange(ctx context.Context, start, end time.Time, limit int) ([]models.SensorData, error) {
	queryStr := fmt.Sprintf(`
        from(bucket: "%s")
        |> range(start: %s, stop: %s)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> limit(n: %d)
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    `, db.bucket, start.Format(time.RFC3339), end.Format(time.RFC3339), limit)

	fmt.Printf(queryStr)
	result, err := db.queryAPI.Query(ctx, queryStr)
	if err != nil {
		return nil, err
	}

	var data []models.SensorData
	for result.Next() {
		record := result.Record()
		sensorData := models.SensorData{
			Timestamp:       record.Time(),
			FactoryID:       getStringValue(record, "factory_id"),
			DeviceID:        getStringValue(record, "device_id"),
			Temperature:     getFloatValue(record, "temperature"),
			Humidity:        getFloatValue(record, "humidity"),
			Pressure:        getFloatValue(record, "pressure"),
			Voltage:         getFloatValue(record, "voltage"),
			Current:         getFloatValue(record, "current"),
			Power:           getFloatValue(record, "power"),
			RPM:             getIntValue(record, "rpm"),
			Status:          getStringValue(record, "status"),
			ErrorCode:       int32(getIntValue(record, "error_code")),
			ProductionCount: getIntValue(record, "production_count"),
		}
		data = append(data, sensorData)
	}

	return data, result.Err()
}

func (db *InfluxDB) QueryGroupBy(ctx context.Context, start, end time.Time, groupBy string, interval time.Duration) (map[string]float32, error) {
	var groupByClause string
	switch groupBy {
	case "device":
		groupByClause = `|> group(columns: ["device_id"])`
	case "factory":
		groupByClause = `|> group(columns: ["factory_id"])`
	default:
		groupByClause = fmt.Sprintf(`|> aggregateWindow(every: %s, fn: mean)`, interval.String())
	}

	queryStr := fmt.Sprintf(`
        from(bucket: "%s")
        |> range(start: %s, stop: %s)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r._field == "temperature")
        %s
        |> mean()
    `, db.bucket, start.Format(time.RFC3339), end.Format(time.RFC3339), groupByClause)
	fmt.Printf(queryStr)
	result, err := db.queryAPI.Query(ctx, queryStr)
	if err != nil {
		return nil, err
	}

	data := make(map[string]float32)
	for result.Next() {
		record := result.Record()
		key := fmt.Sprintf("%v", record.ValueByKey(groupBy+"_id"))
		if key == "<nil>" {
			key = record.Time().Format(time.RFC3339)
		}
		data[key] = getFloatValue(record, "_value")
	}

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

// GetRandomFactoryId 从数据库里获取一个存在的随机的 factoryId
func (db *InfluxDB) GetRandomFactoryId(ctx context.Context) string {
	queryStr := fmt.Sprintf(`
        from(bucket: "%s")
        |> range(start: -30d)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> keep(columns: ["factory_id"])
        |> distinct(column: "factory_id")
        |> limit(n: 1)
    `, db.bucket)

	result, err := db.queryAPI.Query(ctx, queryStr)
	if err != nil {
		return ""
	}

	if result.Next() {
		return getStringValue(result.Record(), "factory_id")
	}

	return ""
}

// GetRandomDeviceId 从数据库里获取一个存在的随机的 deviceId
func (db *InfluxDB) GetRandomDeviceId(ctx context.Context, factoryId string) string {
	var factoryFilter string
	if factoryId != "" {
		factoryFilter = fmt.Sprintf(`|> filter(fn: (r) => r.factory_id == "%s")`, factoryId)
	}

	queryStr := fmt.Sprintf(`
        from(bucket: "%s")
        |> range(start: -30d)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        %s
        |> keep(columns: ["device_id"])
        |> distinct(column: "device_id")
        |> limit(n: 1)
    `, db.bucket, factoryFilter)

	result, err := db.queryAPI.Query(ctx, queryStr)
	if err != nil {
		return ""
	}

	if result.Next() {
		return getStringValue(result.Record(), "device_id")
	}

	return ""
}

// GetStartTime 从数据库里获取指定 factoryId 和 deviceId 的数据的起始时间
func (db *InfluxDB) GetStartTime(ctx context.Context, factoryId string, deviceId string) time.Time {
	var filters []string
	if factoryId != "" {
		filters = append(filters, fmt.Sprintf(`|> filter(fn: (r) => r.factory_id == "%s")`, factoryId))
	}
	if deviceId != "" {
		filters = append(filters, fmt.Sprintf(`|> filter(fn: (r) => r.device_id == "%s")`, deviceId))
	}

	filterStr := ""
	for _, filter := range filters {
		filterStr += filter + "\n        "
	}

	queryStr := fmt.Sprintf(`
        from(bucket: "%s")
        |> range(start: -365d)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        %s
        |> first()
        |> keep(columns: ["_time"])
    `, db.bucket, filterStr)

	result, err := db.queryAPI.Query(ctx, queryStr)
	if err != nil {
		return time.Time{}
	}

	if result.Next() {
		return result.Record().Time()
	}

	return time.Time{}
}

// GetEndTime 从数据库里获取指定 factoryId 和 deviceId 的数据的结束时间
func (db *InfluxDB) GetEndTime(ctx context.Context, factoryId string, deviceId string) time.Time {
	var filters []string
	if factoryId != "" {
		filters = append(filters, fmt.Sprintf(`|> filter(fn: (r) => r.factory_id == "%s")`, factoryId))
	}
	if deviceId != "" {
		filters = append(filters, fmt.Sprintf(`|> filter(fn: (r) => r.device_id == "%s")`, deviceId))
	}

	filterStr := ""
	for _, filter := range filters {
		filterStr += filter + "\n        "
	}

	queryStr := fmt.Sprintf(`
        from(bucket: "%s")
        |> range(start: -365d)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        %s
        |> last()
        |> keep(columns: ["_time"])
    `, db.bucket, filterStr)

	result, err := db.queryAPI.Query(ctx, queryStr)
	if err != nil {
		return time.Time{}
	}

	if result.Next() {
		return result.Record().Time()
	}

	return time.Time{}
}

// RemoveALLData 清除数据库中的所有数据
func (db *InfluxDB) RemoveALLData(ctx context.Context) error {
	// 使用 InfluxDB 的 Delete API 来删除所有数据
	deleteAPI := db.client.DeleteAPI()

	// 删除指定时间范围内的所有数据（这里使用一个很大的时间范围）
	start := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	stop := time.Now().Add(24 * time.Hour) // 明天

	err := deleteAPI.DeleteWithName(ctx, db.org, db.bucket, start, stop, "")
	if err != nil {
		return fmt.Errorf("failed to delete all data: %w", err)
	}

	return nil
}

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

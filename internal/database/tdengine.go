package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"test-benchmark/internal/models"
)

type TDengine struct {
	db       *sql.DB
	dsn      string
	database string
}

func NewTDengine(dsn, database string) *TDengine {
	return &TDengine{
		dsn:      dsn,
		database: database,
	}
}

func (db *TDengine) Name() string {
	return "TDengine"
}

func (db *TDengine) Connect(ctx context.Context) error {
	if db.db != nil {
		// If already connected, just ping to check health
		if err := db.Ping(ctx); err == nil {
			return nil
		}
		// If ping fails, close the existing connection
		if err := db.Close(); err != nil {
			return fmt.Errorf("failed to close existing connection: %w", err)
		}
	}

	var err error
	db.db, err = sql.Open("taosSql", db.dsn)
	if err != nil {
		return err
	}

	// Set connection pool settings
	db.db.SetMaxOpenConns(100)
	db.db.SetMaxIdleConns(20)
	db.db.SetConnMaxLifetime(time.Hour)

	return db.Ping(ctx)
}

func (db *TDengine) Close() error {
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

func (db *TDengine) CreateSchema(ctx context.Context) error {
	queries := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db.database),
		fmt.Sprintf("USE %s", db.database),
		`CREATE STABLE IF NOT EXISTS sensor_data (
            ts TIMESTAMP,
            temperature FLOAT,
            humidity FLOAT,
            pressure FLOAT,
            voltage FLOAT,
            current FLOAT,
            power FLOAT,
            rpm BIGINT,
            status NCHAR(20),
            error_code INT,
            production_count BIGINT
        ) TAGS (
            factory_id NCHAR(50),
            device_id NCHAR(50)
        )`,
	}

	for _, query := range queries {
		_, err := db.db.ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to execute query %s: %w", query, err)
		}
	}

	return nil
}

func (db *TDengine) DropSchema(ctx context.Context) error {
	_, err := db.db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", db.database))
	return err
}

func (db *TDengine) WriteBatch(ctx context.Context, data []models.SensorData) error {
	if len(data) == 0 {
		return nil
	}

	// Use database
	_, err := db.db.ExecContext(ctx, fmt.Sprintf("USE %s", db.database))
	if err != nil {
		return err
	}

	// Group data by device for batch insert
	deviceGroups := make(map[string][]models.SensorData)
	for _, record := range data {
		key := fmt.Sprintf("%s_%s", record.FactoryID, record.DeviceID)
		deviceGroups[key] = append(deviceGroups[key], record)
	}

	for deviceKey, records := range deviceGroups {
		tableName := fmt.Sprintf("sensor_%s", strings.ReplaceAll(deviceKey, "-", "_"))

		// Create table if not exists
		createTableSQL := fmt.Sprintf(`
            CREATE TABLE IF NOT EXISTS %s USING sensor_data TAGS ('%s', '%s')
        `, tableName, records[0].FactoryID, records[0].DeviceID)

		_, err := db.db.ExecContext(ctx, createTableSQL)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}

		// Batch insert
		var values []string
		for _, record := range records {
			value := fmt.Sprintf("('%s', %f, %f, %f, %f, %f, %f, %d, '%s', %d, %d)",
				record.Timestamp.Format("2006-01-02 15:04:05.000"),
				record.Temperature,
				record.Humidity,
				record.Pressure,
				record.Voltage,
				record.Current,
				record.Power,
				record.RPM,
				record.Status,
				record.ErrorCode,
				record.ProductionCount,
			)
			values = append(values, value)
		}

		insertSQL := fmt.Sprintf(`
            INSERT INTO %s VALUES %s
        `, tableName, strings.Join(values, ","))

		_, err = db.db.ExecContext(ctx, insertSQL)
		if err != nil {
			return fmt.Errorf("failed to insert batch data: %w", err)
		}
	}

	return nil
}

func (db *TDengine) QueryByDeviceAndTimeRange(ctx context.Context, deviceID string, start, end time.Time) ([]models.SensorData, error) {
	query := fmt.Sprintf(`
        USE %s;
        SELECT ts, temperature, humidity, pressure, voltage, current, power, rpm, status, error_code, production_count
        FROM sensor_data 
        WHERE device_id = '%s' AND ts >= '%s' AND ts <= '%s'
        ORDER BY ts
    `, db.database, deviceID, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"))

	rows, err := db.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var data []models.SensorData
	for rows.Next() {
		var record models.SensorData
		var ts string

		err := rows.Scan(&ts, &record.Temperature, &record.Humidity, &record.Pressure,
			&record.Voltage, &record.Current, &record.Power, &record.RPM,
			&record.Status, &record.ErrorCode, &record.ProductionCount)
		if err != nil {
			return nil, err
		}

		record.Timestamp, _ = time.Parse("2006-01-02 15:04:05", ts)
		record.DeviceID = deviceID
		data = append(data, record)
	}

	return data, rows.Err()
}

func (db *TDengine) QueryAggregation(ctx context.Context, deviceID string, start, end time.Time, aggType string) (float32, error) {
	var aggFunc string
	switch aggType {
	case "avg":
		aggFunc = "AVG(temperature)"
	case "max":
		aggFunc = "MAX(temperature)"
	case "min":
		aggFunc = "MIN(temperature)"
	default:
		aggFunc = "AVG(temperature)"
	}

	query := fmt.Sprintf(`
        USE %s;
        SELECT %s FROM sensor_data 
        WHERE device_id = '%s' AND ts >= '%s' AND ts <= '%s'
    `, db.database, aggFunc, deviceID, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"))

	var result float32
	err := db.db.QueryRowContext(ctx, query).Scan(&result)
	return result, err
}

func (db *TDengine) QueryTimeRange(ctx context.Context, start, end time.Time, limit int) ([]models.SensorData, error) {
	query := fmt.Sprintf(`
        USE %s;
        SELECT ts, temperature, humidity, pressure, voltage, current, power, rpm, status, error_code, production_count, factory_id, device_id
        FROM sensor_data 
        WHERE ts >= '%s' AND ts <= '%s'
        ORDER BY ts
        LIMIT %d
    `, db.database, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"), limit)

	rows, err := db.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var data []models.SensorData
	for rows.Next() {
		var record models.SensorData
		var ts string

		err := rows.Scan(&ts, &record.Temperature, &record.Humidity, &record.Pressure,
			&record.Voltage, &record.Current, &record.Power, &record.RPM,
			&record.Status, &record.ErrorCode, &record.ProductionCount,
			&record.FactoryID, &record.DeviceID)
		if err != nil {
			return nil, err
		}

		record.Timestamp, _ = time.Parse("2006-01-02 15:04:05", ts)
		data = append(data, record)
	}

	return data, rows.Err()
}

func (db *TDengine) QueryGroupBy(ctx context.Context, start, end time.Time, groupBy string, interval time.Duration) (map[string]float32, error) {
	var query string

	switch groupBy {
	case "device":
		query = fmt.Sprintf(`
            USE %s;
            SELECT device_id, AVG(temperature) FROM sensor_data 
            WHERE ts >= '%s' AND ts <= '%s'
            GROUP BY device_id
        `, db.database, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"))
	case "factory":
		query = fmt.Sprintf(`
            USE %s;
            SELECT factory_id, AVG(temperature) FROM sensor_data 
            WHERE ts >= '%s' AND ts <= '%s'
            GROUP BY factory_id
        `, db.database, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"))
	default:
		// Time-based grouping
		intervalStr := fmt.Sprintf("%ds", int(interval.Seconds()))
		query = fmt.Sprintf(`
            USE %s;
            SELECT _wstart, AVG(temperature) FROM sensor_data 
            WHERE ts >= '%s' AND ts <= '%s'
            INTERVAL(%s)
        `, db.database, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"), intervalStr)
	}

	rows, err := db.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]float32)
	for rows.Next() {
		var key string
		var value float32

		err := rows.Scan(&key, &value)
		if err != nil {
			return nil, err
		}

		result[key] = value
	}

	return result, rows.Err()
}

func (db *TDengine) Ping(ctx context.Context) error {
	return db.db.PingContext(ctx)
}

func (db *TDengine) GetRandomFactoryId(ctx context.Context) string {
	query := fmt.Sprintf(`
        USE %s;
        SELECT DISTINCT factory_id FROM sensor_data LIMIT 1 OFFSET FLOOR(RAND() * (SELECT COUNT(DISTINCT factory_id) FROM sensor_data))
    `, db.database)

	var factoryID string
	err := db.db.QueryRowContext(ctx, query).Scan(&factoryID)
	if err != nil {
		fmt.Errorf("failed to get random factory ID: %w", err)
		return ""
	}
	return factoryID
}

func (db *TDengine) GetRandomDeviceId(ctx context.Context, factoryId string) string {
	query := fmt.Sprintf(`
        USE %s;
        SELECT DISTINCT device_id FROM sensor_data WHERE factory_id = '%s' LIMIT 1 OFFSET FLOOR(RAND() * (SELECT COUNT(DISTINCT device_id) FROM sensor_data WHERE factory_id = '%s'))
    `, db.database, factoryId, factoryId)

	var deviceID string
	err := db.db.QueryRowContext(ctx, query).Scan(&deviceID)
	if err != nil {
		fmt.Errorf("failed to get random device ID: %w", err)
		return ""
	}
	return deviceID
}

func (db *TDengine) GetStartTime(ctx context.Context, factoryId string, deviceId string) time.Time {
	query := fmt.Sprintf(`
        USE %s;
        SELECT MIN(ts) FROM sensor_data WHERE factory_id = '%s' AND device_id = '%s'
    `, db.database, factoryId, deviceId)

	var startTime string
	err := db.db.QueryRowContext(ctx, query).Scan(&startTime)
	if err != nil {
		fmt.Errorf("failed to get start time: %w", err)
		return time.Time{}
	}

	parsedTime, err := time.Parse("2006-01-02 15:04:05", startTime)
	if err != nil {
		fmt.Errorf("failed to parse start time: %w", err)
		return time.Time{}
	}
	return parsedTime
}

func (db *TDengine) GetEndTime(ctx context.Context, factoryId string, deviceId string) time.Time {
	query := fmt.Sprintf(`
        USE %s;
        SELECT MAX(ts) FROM sensor_data WHERE factory_id = '%s' AND device_id = '%s'
    `, db.database, factoryId, deviceId)

	var endTime string
	err := db.db.QueryRowContext(ctx, query).Scan(&endTime)
	if err != nil {
		fmt.Errorf("failed to get end time: %w", err)
		return time.Time{}
	}

	parsedTime, err := time.Parse("2006-01-02 15:04:05", endTime)
	if err != nil {
		fmt.Errorf("failed to parse end time: %w", err)
		return time.Time{}
	}
	return parsedTime
}

func (db *TDengine) RemoveALLData(ctx context.Context) error {
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", db.database)
	_, err := db.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to remove all data: %w", err)
	}

	// Optionally recreate the database
	createDbQuery := fmt.Sprintf("CREATE DATABASE %s", db.database)
	_, err = db.db.ExecContext(ctx, createDbQuery)
	if err != nil {
		return fmt.Errorf("failed to recreate database: %w", err)
	}

	return nil
}

package database

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"test-benchmark/internal/models"
	"time"

	"github.com/go-resty/resty/v2"
)

type TDengineDB struct {
	client   *resty.Client
	host     string
	port     int
	username string
	password string
	database string
	token    string
	logger   *log.Logger
}

type TDengineResponse struct {
	Code       int             `json:"code"`
	ColumnMeta [][]interface{} `json:"column_meta,omitempty"`
	Data       [][]interface{} `json:"data,omitempty"`
	Rows       int             `json:"rows,omitempty"`
	Desc       string          `json:"desc,omitempty"`
}

func NewTDengineDB(host string, port int, username string, password string, db string) *TDengineDB {
	client := resty.New()

	// 设置超时时间
	client.SetTimeout(30 * time.Second)

	// 配置连接池和复用
	client.SetTransport(&http.Transport{
		MaxIdleConns:        100,              // 最大空闲连接数
		MaxIdleConnsPerHost: 20,               // 每个主机的最大空闲连接数
		IdleConnTimeout:     90 * time.Second, // 空闲连接超时
		DisableKeepAlives:   false,            // 启用连接复用
		MaxConnsPerHost:     50,               // 每个主机的最大连接数
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second, // 连接超时
			KeepAlive: 30 * time.Second, // 保持连接
		}).DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	})

	// 设置重试机制
	client.SetRetryCount(3)
	client.SetRetryWaitTime(500 * time.Millisecond)
	client.SetRetryMaxWaitTime(2 * time.Second)

	return &TDengineDB{
		client:   client,
		host:     host,
		port:     port,
		username: username,
		password: password,
		database: db,
		logger:   log.New(os.Stdout, "[TDengine] ", log.LstdFlags),
	}
}

func (td *TDengineDB) Name() string {
	return "TDengine"
}

func (td *TDengineDB) Connect(ctx context.Context) error {
	// 使用 Basic Auth，直接设置认证信息
	td.client.SetBasicAuth(td.username, td.password)

	// 测试连接
	if err := td.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to TDengine: %w", err)
	}

	return nil
}

func (td *TDengineDB) Close() error {
	return nil
}

func (td *TDengineDB) executeSQL(ctx context.Context, sql string) (*TDengineResponse, error) {
	td.logger.Printf("Executing SQL: %s", sql)

	url := fmt.Sprintf("http://%s:%d/rest/sql/%s", td.host, td.port, td.database)

	resp, err := td.client.R().
		SetContext(ctx).
		SetHeader("Content-Type", "text/plain").
		SetBody(sql).
		Post(url)

	if err != nil {
		td.logger.Printf("HTTP request failed: %v", err)
		return nil, fmt.Errorf("failed to execute SQL: %w", err)
	}

	td.logger.Printf("Response status: %d", resp.StatusCode())

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("HTTP error: %d - %s", resp.StatusCode(), string(resp.Body()))
	}

	var response TDengineResponse
	if err := json.Unmarshal(resp.Body(), &response); err != nil {
		td.logger.Printf("Failed to parse response: %v", err)
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if response.Code != 0 {
		td.logger.Printf("TDengine error: Code %d, Message: %s", response.Code, response.Desc)
		return nil, fmt.Errorf("SQL execution failed (code: %d): %s", response.Code, response.Desc)
	}

	td.logger.Printf("Query executed successfully, rows affected: %d", response.Rows)
	return &response, nil
}

func (td *TDengineDB) CreateSchema(ctx context.Context) error {
	// 创建数据库
	createDbSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", td.database)
	if _, err := td.executeSQL(ctx, createDbSQL); err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// 创建超级表（注意：URL中已经指定了数据库，所以需要带数据库前缀）
	createStableSQL := fmt.Sprintf(`
    CREATE STABLE IF NOT EXISTS %s.sensor_data (
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
        production_count BIGINT,
        job_id NCHAR(64)
    ) TAGS (
        factory_id NCHAR(32),
        device_id NCHAR(32)
    )`, td.database)

	if _, err := td.executeSQL(ctx, createStableSQL); err != nil {
		return fmt.Errorf("failed to create stable: %w", err)
	}

	return nil
}

func (td *TDengineDB) DropSchema(ctx context.Context) error {
	dropDbSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s", td.database)
	if _, err := td.executeSQL(ctx, dropDbSQL); err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}
	return nil
}

func (td *TDengineDB) WriteBatch(ctx context.Context, data []models.SensorData) error {
	if len(data) == 0 {
		return nil
	}

	// 按批次处理，避免单次请求过大
	batchSize := 1000
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		if err := td.writeSingleBatch(ctx, batch); err != nil {
			return fmt.Errorf("failed to write batch %d-%d: %w", i, end-1, err)
		}
	}

	return nil
}

func (td *TDengineDB) writeSingleBatch(ctx context.Context, data []models.SensorData) error {
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("INSERT INTO ")

	for i, record := range data {
		tableName := fmt.Sprintf("%s.sensor_%s_%s",
			td.database,
			strings.ReplaceAll(record.FactoryID, "-", "_"),
			strings.ReplaceAll(record.DeviceID, "-", "_"))

		if i > 0 {
			sqlBuilder.WriteString(" ")
		}

		sqlBuilder.WriteString(fmt.Sprintf(
			"%s USING %s.sensor_data TAGS ('%s', '%s') VALUES (%d, %f, %f, %f, %f, %f, %f, %d, '%s', %d, %d, '%s')",
			tableName,
			td.database,
			record.FactoryID,
			record.DeviceID,
			record.Timestamp.UnixMilli(),
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
			record.JobId,
		))
	}

	_, err := td.executeSQL(ctx, sqlBuilder.String())
	return err
}

func (td *TDengineDB) QueryByDeviceAndTimeRange(ctx context.Context, jobId string, deviceID string, start, end time.Time) ([]models.SensorData, error) {
	sql := fmt.Sprintf(`
        SELECT ts, temperature, humidity, pressure, voltage, current, power, rpm, status, error_code, production_count, job_id, factory_id, device_id
        FROM %s.sensor_data 
        WHERE device_id = '%s' AND job_id = '%s' AND ts >= '%s' AND ts <= '%s'
        ORDER BY ts
        `,
		td.database, deviceID, jobId, start.Format("2006-01-02 15:04:05.000"), end.Format("2006-01-02 15:04:05.000"))

	response, err := td.executeSQL(ctx, sql)
	if err != nil {
		return nil, err
	}

	return td.parseQueryResponse(response)
}

func (td *TDengineDB) QueryAggregation(ctx context.Context, jobId string, deviceID string, start, end time.Time, aggType string) (float32, error) {
	sql := fmt.Sprintf(`
        SELECT %s(temperature) as result
        FROM %s.sensor_data 
        WHERE device_id = '%s' AND job_id = '%s' AND ts >= '%s' AND ts <= '%s'    INTERVAL(1m) `,
		aggType, td.database, deviceID, jobId, start.Format("2006-01-02 15:04:05.000"), end.Format("2006-01-02 15:04:05.000"))

	response, err := td.executeSQL(ctx, sql)
	if err != nil {
		return 0, err
	}

	if len(response.Data) == 0 || len(response.Data[0]) == 0 {
		return 0, nil
	}

	result, ok := response.Data[0][0].(float64)
	if !ok {
		return 0, fmt.Errorf("invalid result type")
	}

	return float32(result), nil
}

func (td *TDengineDB) QueryTimeRange(ctx context.Context, jobId string, start, end time.Time, limit int) ([]models.SensorData, error) {
	/*        SELECT ts, temperature, humidity, pressure, voltage, current, power, rpm, status, error_code, production_count, job_id, factory_id, device_id
	FROM %s.sensor_data
	WHERE job_id = '%s' AND ts >= '%s' AND ts <= '%s'
	ORDER BY ts
	LIMIT %d*/
	sql := fmt.Sprintf(`
        SELECT _wstart, avg(temperature) 
	    FROM %s.sensor_data 
        WHERE ts >= '%s' AND ts <= '%s' AND job_id = '%s'
		INTERVAL(1m)
         ORDER BY _wstart
        LIMIT %d
        `,
		td.database, start.Format("2006-01-02 15:04:05.000"), end.Format("2006-01-02 15:04:05.000"), jobId, limit)

	response, err := td.executeSQL(ctx, sql)
	if err != nil {
		return nil, err
	}

	return td.parseQueryResponse(response)
}

func (td *TDengineDB) QueryGroupBy(ctx context.Context, jobId string, start, end time.Time, groupBy string, interval time.Duration) (map[string]float32, error) {
	intervalStr := fmt.Sprintf("%ds", int(interval.Seconds()))

	sql := fmt.Sprintf(`
        SELECT _wstart, %s, AVG(temperature) as avg_temp
        FROM %s.sensor_data 
        WHERE job_id = '%s' AND ts >= '%s' AND ts <= '%s'
        PARTITION BY %s
        INTERVAL(%s)
        `,
		groupBy, td.database, jobId, start.Format("2006-01-02 15:04:05.000"), end.Format("2006-01-02 15:04:05.000"), groupBy, intervalStr)

	response, err := td.executeSQL(ctx, sql)
	if err != nil {
		return nil, err
	}

	result := make(map[string]float32)
	for _, row := range response.Data {
		if len(row) >= 3 {
			key := fmt.Sprintf("%v_%v", row[0], row[1])
			if val, ok := row[2].(float64); ok {
				result[key] = float32(val)
			}
		}
	}

	return result, nil
}

func (td *TDengineDB) Ping(ctx context.Context) error {
	sql := "SELECT SERVER_STATUS()"
	_, err := td.executeSQL(ctx, sql)
	return err
}

func (td *TDengineDB) parseQueryResponse(response *TDengineResponse) ([]models.SensorData, error) {
	var results []models.SensorData

	for _, row := range response.Data {
		if len(row) < 14 {
			continue
		}

		var data models.SensorData

		// 解析时间戳 - TDengine 返回格式为 RFC3339
		if ts, ok := row[0].(string); ok {
			if parsedTime, err := time.Parse(time.RFC3339, ts); err == nil {
				data.Timestamp = parsedTime
			}
		}

		// 解析其他字段
		if val, ok := row[1].(float64); ok {
			data.Temperature = float32(val)
		}
		if val, ok := row[2].(float64); ok {
			data.Humidity = float32(val)
		}
		if val, ok := row[3].(float64); ok {
			data.Pressure = float32(val)
		}
		if val, ok := row[4].(float64); ok {
			data.Voltage = float32(val)
		}
		if val, ok := row[5].(float64); ok {
			data.Current = float32(val)
		}
		if val, ok := row[6].(float64); ok {
			data.Power = float32(val)
		}
		if val, ok := row[7].(float64); ok {
			data.RPM = int64(val)
		}
		if val, ok := row[8].(string); ok {
			data.Status = val
		}
		if val, ok := row[9].(float64); ok {
			data.ErrorCode = int32(val)
		}
		if val, ok := row[10].(float64); ok {
			data.ProductionCount = int64(val)
		}
		if val, ok := row[11].(string); ok {
			data.JobId = val
		}
		if val, ok := row[12].(string); ok {
			data.FactoryID = val
		}
		if val, ok := row[13].(string); ok {
			data.DeviceID = val
		}

		results = append(results, data)
	}

	return results, nil
}

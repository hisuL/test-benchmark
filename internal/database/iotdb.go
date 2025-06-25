package database

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/common"
	"test-benchmark/internal/models"
)

type IoTDB struct {
	sessionPool        client.TableSessionPool // 表模型会话池
	host               string
	port               int
	username           string
	password           string
	database           string // 表模型需要指定数据库
	tableCreated       bool
	tableCreationMutex sync.Mutex
	hasPool            bool
}

func NewIoTDB(host string, port int, username, password, database string) *IoTDB {
	return &IoTDB{
		host:         host,
		port:         port,
		username:     username,
		password:     password,
		database:     database,
		tableCreated: false,
		hasPool:      false,
	}
}

func (db *IoTDB) sessionPoolConnect(ctx context.Context) {
	config := &client.PoolConfig{
		Host:     db.host,
		Port:     strconv.Itoa(db.port),
		UserName: db.username,
		Password: db.password,
	}
	db.sessionPool = client.NewTableSessionPool(config, 3, 60000, 8000, false)

}

func (db *IoTDB) Name() string {
	return "IoTDB"
}

func (db *IoTDB) Connect(ctx context.Context) error {
	db.CreateSchema(ctx)
	if db.hasPool {
		return nil
	}

	config := &client.PoolConfig{
		Host:     db.host,
		Port:     strconv.Itoa(db.port),
		UserName: db.username,
		Password: db.password,
		Database: db.database,
	}

	// 创建表模型会话池
	db.sessionPool = client.NewTableSessionPool(config, 50, 60000, 30000, false)
	db.hasPool = true
	return db.Ping(ctx)
}

func (db *IoTDB) Close() error {
	if db.hasPool {
		db.sessionPool.Close()
	}
	return nil
}

func (db *IoTDB) CreateSchema(ctx context.Context) error {
	if db.hasPool {
		fmt.Println("Session pool already exists, skipping CreateSchema")
		return nil
	}
	db.sessionPoolConnect(ctx)
	session, err := db.sessionPool.GetSession()
	if err != nil {
		return err
	}

	// 检查数据库是否存在，不存在则创建
	timeout := int64(30000)
	fmt.Printf("SHOW DATABASES at %s:%d with user %s\n", db.host, db.port, db.username)
	dataSet, err := session.ExecuteQueryStatement("SHOW DATABASES", &timeout)
	if err == nil {
		defer dataSet.Close()

		existingDBs := make(map[string]bool)
		for {
			hasNext, err := dataSet.Next()
			if err != nil || !hasNext {
				break
			}
			dbName, err := dataSet.GetString("Database")
			fmt.Printf("Found database: %s\n", dbName)
			if err == nil {
				existingDBs[dbName] = true
			}
		}

		// 如果数据库不存在，创建它
		if !existingDBs[db.database] {
			status, err := session.ExecuteNonQueryStatement(fmt.Sprintf("CREATE DATABASE %s", db.database))
			fmt.Printf(fmt.Sprintf("CREATE DATABASE %s", db.database))
			fmt.Printf("status:" + status.GetMessage() + "\n")
			if err != nil {
				return err
			}
			if err = checkError(status, err); err != nil {
				return err
			}
		}
	}
	if err != nil {
		fmt.Printf("SHOW DATABASES failed: %s\n", err)
	}

	fmt.Printf("USE RIGHT DATABASE at %s:%d with user %s\n", db.host, db.port, db.username)
	checkError(session.ExecuteNonQueryStatement("use " + db.database))
	// 创建单一的sensor_data表
	err = db.ensureTableExists(session)
	if err != nil {
		return err
	}
	db.sessionPool.Close()
	return nil
}

func (db *IoTDB) DropSchema(ctx context.Context) error {

	return nil
}

func (db *IoTDB) WriteBatch(ctx context.Context, data []models.SensorData) error {
	if len(data) == 0 {
		return nil
	}

	session, err := db.sessionPool.GetSession()
	if err != nil {
		return fmt.Errorf("failed to get session: %w", err)
	}
	defer session.Close()

	// 确保表存在
	err = db.ensureTableExists(session)
	if err != nil {
		return fmt.Errorf("failed to ensure table exists: %w", err)
	}

	// 构建批量插入SQL
	var valueStrings []string
	for _, record := range data {
		valueString := fmt.Sprintf(
			"('%s', '%s', %d, %f, %f, %f, %f, %f, %f, %d, '%s', %d, %d, '%s')",
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
			strings.ReplaceAll(record.Status, "'", "''"), // 转义单引号
			record.ErrorCode,
			record.ProductionCount,
			strings.ReplaceAll(record.JobId, "'", "''"), // 转义单引号
		)
		valueStrings = append(valueStrings, valueString)
	}

	// 批量插入到单一表
	insertSQL := fmt.Sprintf(
		"INSERT INTO sensor_data (factory_id, device_id, time, temperature, humidity, pressure, voltage, current, power, rpm, status, error_code, production_count, job_id) VALUES %s",
		strings.Join(valueStrings, ", "),
	)

	status, err := session.ExecuteNonQueryStatement(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}
	if err = checkError(status, err); err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	return nil
}

// 确保sensor_data表存在
func (db *IoTDB) ensureTableExists(session client.ITableSession) error {
	db.tableCreationMutex.Lock()
	if db.tableCreated {
		db.tableCreationMutex.Unlock()
		return nil
	}
	db.tableCreationMutex.Unlock()

	// 创建sensor_data表的SQL
	createTableSQL := `
        CREATE TABLE IF NOT EXISTS sensor_data (
            factory_id STRING TAG,
            device_id STRING TAG,
            temperature FLOAT FIELD,
            humidity FLOAT FIELD,
            pressure FLOAT FIELD,
            voltage FLOAT FIELD,
            current FLOAT FIELD,
            power FLOAT FIELD,
            rpm INT64 FIELD,
            status STRING FIELD,
            error_code INT32 FIELD,
            production_count INT64 FIELD,
            job_id STRING FIELD
        )`

	status, err := session.ExecuteNonQueryStatement(createTableSQL)
	if err != nil {
		return err
	}
	if err = checkError(status, err); err != nil {
		return err
	}

	// 记录表已创建
	db.tableCreationMutex.Lock()
	db.tableCreated = true
	db.tableCreationMutex.Unlock()

	return nil
}

func (db *IoTDB) QueryByDeviceAndTimeRange(ctx context.Context, jobId string, deviceID string, start, end time.Time) ([]models.SensorData, error) {
	session, err := db.sessionPool.GetSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()

	// 查询单一表
	sql := fmt.Sprintf(`
        SELECT factory_id, device_id, time, temperature, humidity, pressure, voltage, current, power, rpm, status, error_code, production_count, job_id
        FROM sensor_data
        WHERE device_id = '%s' AND time >= %d AND time <= %d AND job_id = '%s'
        ORDER BY time
    `, deviceID, start.UnixMilli(), end.UnixMilli(), jobId)

	fmt.Printf("SQL: %s\n", sql)
	timeout := int64(60000)
	dataSet, err := session.ExecuteQueryStatement(sql, &timeout)
	if err != nil {
		return nil, err
	}
	defer dataSet.Close()

	var data []models.SensorData
	for {
		hasNext, err := dataSet.Next()
		if err != nil {
			return nil, fmt.Errorf("error reading next record: %w", err)
		}
		if !hasNext {
			break
		}

		factoryID, _ := dataSet.GetString("factory_id")
		deviceIDResult, _ := dataSet.GetString("device_id")
		timestamp, _ := dataSet.GetLong("ts")
		temperature, _ := dataSet.GetFloat("temperature")
		humidity, _ := dataSet.GetFloat("humidity")
		pressure, _ := dataSet.GetFloat("pressure")
		voltage, _ := dataSet.GetFloat("voltage")
		current, _ := dataSet.GetFloat("current")
		power, _ := dataSet.GetFloat("power")
		rpm, _ := dataSet.GetLong("rpm")
		status, _ := dataSet.GetString("status")
		errorCode, _ := dataSet.GetInt("error_code")
		productionCount, _ := dataSet.GetLong("production_count")
		jobIdResult, _ := dataSet.GetString("job_id")

		sensorData := models.SensorData{
			Timestamp:       time.UnixMilli(timestamp),
			FactoryID:       factoryID,
			DeviceID:        deviceIDResult,
			Temperature:     float32(temperature),
			Humidity:        float32(humidity),
			Pressure:        float32(pressure),
			Voltage:         float32(voltage),
			Current:         float32(current),
			Power:           float32(power),
			RPM:             rpm,
			Status:          status,
			ErrorCode:       int32(errorCode),
			ProductionCount: productionCount,
			JobId:           jobIdResult,
		}
		data = append(data, sensorData)
	}

	fmt.Println("QueryByDeviceAndTimeRange data size:", len(data))
	return data, nil
}

func (db *IoTDB) QueryAggregation(ctx context.Context, jobId string, deviceID string, start, end time.Time, aggType string) (float32, error) {
	session, err := db.sessionPool.GetSession()
	if err != nil {
		return 0, err
	}
	defer session.Close()

	var aggFunc string
	switch aggType {
	case "avg":
		aggFunc = "AVG"
	case "max":
		aggFunc = "MAX"
	case "min":
		aggFunc = "MIN"
	default:
		aggFunc = "AVG"
	}

	sql := fmt.Sprintf(`
        SELECT %s(temperature) as agg_value
        FROM sensor_data
        WHERE device_id = '%s' AND time >= %d AND time <= %d AND job_id = '%s GROUP BY  date_bin(1m, time) '
    `, aggFunc, deviceID, start.UnixMilli(), end.UnixMilli(), jobId)

	fmt.Printf("SQL: %s\n", sql)
	timeout := int64(60000)
	dataSet, err := session.ExecuteQueryStatement(sql, &timeout)
	if err != nil {
		return 0, err
	}
	defer dataSet.Close()

	hasNext, err := dataSet.Next()
	if err != nil || !hasNext {
		return 0, fmt.Errorf("no aggregation result found")
	}

	aggValue, err := dataSet.GetFloat("agg_value")
	if err != nil {
		return 0, err
	}

	fmt.Printf("QueryAggregation result: %f\n", aggValue)
	return float32(aggValue), nil
}

func (db *IoTDB) QueryTimeRange(ctx context.Context, jobId string, start, end time.Time, limit int) ([]models.SensorData, error) {
	session, err := db.sessionPool.GetSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()

	sql := fmt.Sprintf(`
        SELECT AVG(temperature) 
        FROM sensor_data
        WHERE time >= %d AND time <= %d AND job_id = '%s'
        GROUP BY  date_bin(1m, time)
        LIMIT %d
    `, start.UnixMilli(), end.UnixMilli(), jobId, limit)

	fmt.Printf("SQL: %s\n", sql)
	timeout := int64(60000)
	dataSet, err := session.ExecuteQueryStatement(sql, &timeout)
	if err != nil {
		return nil, err
	}
	defer dataSet.Close()

	var data []models.SensorData
	for {
		hasNext, err := dataSet.Next()
		if err != nil {
			return nil, fmt.Errorf("error reading next record: %w", err)
		}
		if !hasNext {
			break
		}

		factoryID, _ := dataSet.GetString("factory_id")
		deviceID, _ := dataSet.GetString("device_id")
		timestamp, _ := dataSet.GetLong("ts")
		temperature, _ := dataSet.GetFloat("temperature")
		humidity, _ := dataSet.GetFloat("humidity")
		pressure, _ := dataSet.GetFloat("pressure")
		voltage, _ := dataSet.GetFloat("voltage")
		current, _ := dataSet.GetFloat("current")
		power, _ := dataSet.GetFloat("power")
		rpm, _ := dataSet.GetLong("rpm")
		status, _ := dataSet.GetString("status")
		errorCode, _ := dataSet.GetInt("error_code")
		productionCount, _ := dataSet.GetLong("production_count")
		jobIdResult, _ := dataSet.GetString("job_id")

		sensorData := models.SensorData{
			Timestamp:       time.UnixMilli(timestamp),
			FactoryID:       factoryID,
			DeviceID:        deviceID,
			Temperature:     float32(temperature),
			Humidity:        float32(humidity),
			Pressure:        float32(pressure),
			Voltage:         float32(voltage),
			Current:         float32(current),
			Power:           float32(power),
			RPM:             rpm,
			Status:          status,
			ErrorCode:       int32(errorCode),
			ProductionCount: productionCount,
			JobId:           jobIdResult,
		}

		data = append(data, sensorData)
	}

	fmt.Println("QueryTimeRange data size:", len(data))
	return data, nil
}

func (db *IoTDB) QueryGroupBy(ctx context.Context, jobId string, start, end time.Time, groupBy string, interval time.Duration) (map[string]float32, error) {
	session, err := db.sessionPool.GetSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()

	var sql string
	timeout := int64(60000)

	switch groupBy {
	case "device":
		sql = fmt.Sprintf(`
            SELECT device_id, AVG(temperature) as avg_temp
            FROM sensor_data
            WHERE time >= %d AND time <= %d AND job_id = '%s'
            GROUP BY device_id,  date_bin(1m, time)
        `, start.UnixMilli(), end.UnixMilli(), jobId)
	case "factory":
		sql = fmt.Sprintf(`
            SELECT factory_id, AVG(temperature) as avg_temp
            FROM sensor_data
            WHERE time >= %d AND time <= %d AND job_id = '%s'
            GROUP BY factory_id,  date_bin(1m, time)
        `, start.UnixMilli(), end.UnixMilli(), jobId)
	case "status":
		sql = fmt.Sprintf(`
            SELECT status, AVG(temperature) as avg_temp
            FROM sensor_data 
            WHERE time >= %d AND time <= %d AND job_id = '%s'
            GROUP BY status,  date_bin(1m, time)
        `, start.UnixMilli(), end.UnixMilli(), jobId)
	case "time":
		// 表模型中的时间分组需要使用不同的语法
		intervalMs := interval.Milliseconds()
		sql = fmt.Sprintf(`
            SELECT (time / %d) * %d as time_bucket, AVG(temperature) as avg_temp
            FROM sensor_data
            WHERE time >= %d AND time <= %d AND job_id = '%s'
            GROUP BY time_bucket
            ORDER BY time_bucket
        `, intervalMs, intervalMs, start.UnixMilli(), end.UnixMilli(), jobId)
	default:
		return nil, fmt.Errorf("unsupported groupBy type: %s", groupBy)
	}

	fmt.Printf("SQL: %s\n", sql)
	dataSet, err := session.ExecuteQueryStatement(sql, &timeout)
	if err != nil {
		return nil, err
	}
	defer dataSet.Close()

	result := make(map[string]float32)
	for {
		hasNext, err := dataSet.Next()
		if err != nil {
			return nil, fmt.Errorf("error reading next record: %w", err)
		}
		if !hasNext {
			break
		}

		var key string
		var avgTemp float32

		switch groupBy {
		case "device":
			key, _ = dataSet.GetString("device_id")
			avgTemp, _ = dataSet.GetFloat("avg_temp")
		case "factory":
			key, _ = dataSet.GetString("factory_id")
			avgTemp, _ = dataSet.GetFloat("avg_temp")
		case "status":
			key, _ = dataSet.GetString("status")
			avgTemp, _ = dataSet.GetFloat("avg_temp")
		case "time":
			timeBucket, _ := dataSet.GetLong("time_bucket")
			key = time.UnixMilli(timeBucket).Format(time.RFC3339)
			avgTemp, _ = dataSet.GetFloat("avg_temp")
		}

		result[key] = float32(avgTemp)
	}

	fmt.Println("QueryGroupBy result size:", len(result))
	return result, nil
}

func (db *IoTDB) Ping(ctx context.Context) error {
	session, err := db.sessionPool.GetSession()
	if err != nil {
		return err
	}
	defer session.Close()

	timeout := int64(10000)
	dataSet, err := session.ExecuteQueryStatement("SHOW DATABASES", &timeout)
	if err != nil {
		return err
	}
	defer dataSet.Close()
	return nil
}

// 错误检查函数
func checkError(status *common.TSStatus, err error) error {
	if err != nil {
		return err
	}

	if status != nil {
		if err = client.VerifySuccess(status); err != nil {
			return err
		}
	}

	return nil
}

// 辅助函数：格式化时间间隔（如果需要的话）
func formatInterval(interval time.Duration) string {
	if interval >= 24*time.Hour {
		days := int(interval.Hours() / 24)
		return fmt.Sprintf("%dd", days)
	} else if interval >= time.Hour {
		hours := int(interval.Hours())
		return fmt.Sprintf("%dh", hours)
	} else if interval >= time.Minute {
		minutes := int(interval.Minutes())
		return fmt.Sprintf("%dm", minutes)
	} else if interval >= time.Second {
		seconds := int(interval.Seconds())
		return fmt.Sprintf("%ds", seconds)
	} else {
		milliseconds := int(interval.Milliseconds())
		return fmt.Sprintf("%dms", milliseconds)
	}
}

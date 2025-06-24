package database

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iotdb-client-go/client"
	"github.com/apache/iotdb-client-go/common"
	"test-benchmark/internal/models"
)

type IoTDB struct {
	sessionPool             client.SessionPool // 改为直接使用类型而不是指针
	host                    string
	port                    int
	username                string
	password                string
	createdTimeseries       map[string]bool
	timeseriesCreationMutex sync.Mutex
	hasPool                 bool // 是否有可用的sessionPool
}

func NewIoTDB(host string, port int, username, password string) *IoTDB {
	return &IoTDB{
		host:              host,
		port:              port,
		username:          username,
		password:          password,
		createdTimeseries: make(map[string]bool),
		hasPool:           false,
	}
}

func (db *IoTDB) Name() string {
	return "IoTDB"
}

func (db *IoTDB) Connect(ctx context.Context) error {
	// 如果sessionPool已经存在且可用，先验证连接有效性
	if db.hasPool {
		return nil
	}

	config := &client.PoolConfig{
		Host:     db.host,
		Port:     strconv.Itoa(db.port),
		UserName: db.username,
		Password: db.password,
	}

	// 创建sessionPool - 直接赋值而不是取地址
	db.sessionPool = client.NewSessionPool(config, 50, 60000, 30000, false)
	db.hasPool = true
	return db.Ping(ctx)
}

func (db *IoTDB) Close() error {
	db.sessionPool.Close()
	return nil
}

func (db *IoTDB) CreateSchema(ctx context.Context) error {
	session, err := db.sessionPool.GetSession()
	if err != nil {
		return err
	}
	defer db.sessionPool.PutBack(session)

	// 使用 SHOW STORAGE GROUP 而不是 SHOW DATABASES（兼容性更好）
	dataSet, err := session.ExecuteStatement("SHOW STORAGE GROUP")
	if err != nil {
		return err
	}
	defer dataSet.Close()

	existingDBs := make(map[string]bool)
	for next, err := dataSet.Next(); err == nil && next; next, err = dataSet.Next() {
		// 按列名获取值，更安全
		if dbName := dataSet.GetText("storage group"); err == nil {
			existingDBs[dbName] = true
		}
	}

	// 创建存储组
	factoryPrefixes := []string{"factory_001", "factory_002", "factory_003"}
	for _, factory := range factoryPrefixes {
		storageGroup := fmt.Sprintf("root.%s", factory)
		if !existingDBs[storageGroup] {
			err = checkError(session.SetStorageGroup(storageGroup))
			if err != nil && !strings.Contains(err.Error(), "already exists") {
				return err
			}
		}
	}

	return nil
}

func (db *IoTDB) DropSchema(ctx context.Context) error {
	session, err := db.sessionPool.GetSession()
	if err != nil {
		return err
	}
	defer db.sessionPool.PutBack(session)

	dataSet, err := session.ExecuteStatement("SHOW STORAGE GROUP")
	if err != nil {
		return err
	}
	defer dataSet.Close()

	var storageGroups []string
	for next, err := dataSet.Next(); err == nil && next; next, err = dataSet.Next() {
		sgName := dataSet.GetText("storage group")
		if sgName != "" {
			if strings.Contains(sgName, "factory_") {
				storageGroups = append(storageGroups, sgName)
			}
		}
	}

	// 批量删除存储组
	if len(storageGroups) > 0 {
		return checkError(session.DeleteStorageGroups(storageGroups...))
	}

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
	defer db.sessionPool.PutBack(session)

	// 按工厂和设备分组，使每个设备的数据放在一起处理
	factoryDeviceGroups := make(map[string][]models.SensorData)
	for _, record := range data {
		key := fmt.Sprintf("%s.%s", record.FactoryID, record.DeviceID)
		factoryDeviceGroups[key] = append(factoryDeviceGroups[key], record)
	}

	// 处理每个设备的批量数据
	for deviceKey, records := range factoryDeviceGroups {
		parts := strings.Split(deviceKey, ".")
		if len(parts) != 2 {
			return fmt.Errorf("invalid device key format: %s", deviceKey)
		}

		factoryID := parts[0]
		deviceID := parts[1]

		// 使用工厂ID作为存储组，设备ID作为设备路径
		// 规范化工厂和设备ID，移除任何非法字符
		safeFactoryID := strings.ReplaceAll(factoryID, "-", "_")
		safeDeviceID := strings.ReplaceAll(deviceID, "-", "_")
		devicePath := fmt.Sprintf("root.%s.%s", safeFactoryID, safeDeviceID)

		// 准备对齐时间序列批量写入的参数
		timestamps := make([]int64, 0, len(records))
		measurementsList := make([][]string, 0, len(records))
		valuesList := make([][]interface{}, 0, len(records))
		typesList := make([][]client.TSDataType, 0, len(records))
		deviceIds := make([]string, 0, len(records))

		// 固定的测量点名称和数据类型
		measurements := []string{
			"temperature", "humidity", "pressure", "voltage",
			"current", "power", "rpm", "status",
			"error_code", "production_count", "job_id",
		}

		types := []client.TSDataType{
			client.FLOAT, client.FLOAT, client.FLOAT, client.FLOAT,
			client.FLOAT, client.FLOAT, client.INT64, client.TEXT,
			client.INT32, client.INT64, client.TEXT,
		}

		// 预创建对齐时间序列（如果不存在）
		err := db.ensureAlignedTimeseriesExists(session, devicePath, measurements, types)
		if err != nil {
			return fmt.Errorf("failed to ensure aligned timeseries exists: %w", err)
		}

		// 为每条记录整理数据
		for _, record := range records {
			deviceIds = append(deviceIds, devicePath)
			timestamps = append(timestamps, record.Timestamp.UnixMilli())

			values := []interface{}{
				float32(record.Temperature), float32(record.Humidity),
				float32(record.Pressure), float32(record.Voltage),
				float32(record.Current), float32(record.Power),
				int64(record.RPM), record.Status,
				int32(record.ErrorCode), int64(record.ProductionCount), record.JobId,
			}

			measurementsList = append(measurementsList, measurements)
			valuesList = append(valuesList, values)
			typesList = append(typesList, types)
		}

		// 使用对齐时间序列方式批量插入数据
		_, err = session.InsertAlignedRecords(deviceIds, measurementsList, typesList, valuesList, timestamps)
		if err != nil {
			return fmt.Errorf("failed to insert aligned records for %s: %w", deviceKey, err)
		}
	}

	return nil
}

// 确保对齐时间序列存在，不存在则创建
func (db *IoTDB) ensureAlignedTimeseriesExists(session client.Session, devicePath string, measurements []string, types []client.TSDataType) error {
	// 使用缓存机制避免重复创建
	cacheKey := devicePath
	db.timeseriesCreationMutex.Lock()
	if _, exists := db.createdTimeseries[cacheKey]; exists {
		db.timeseriesCreationMutex.Unlock()
		return nil
	}
	db.timeseriesCreationMutex.Unlock()

	// 准备创建对齐时间序列的编码和压缩方式
	encodings := make([]client.TSEncoding, len(measurements))
	compressions := make([]client.TSCompressionType, len(measurements))

	for i, dataType := range types {
		// 根据数据类型选择最佳编码
		switch dataType {
		case client.FLOAT:
			encodings[i] = client.GORILLA
		case client.INT32, client.INT64:
			encodings[i] = client.TS_2DIFF
		case client.TEXT:
			encodings[i] = client.PLAIN
		default:
			encodings[i] = client.PLAIN
		}
		// 使用LZ4压缩
		compressions[i] = client.LZ4
	}

	// 创建对齐时间序列
	_, err := session.CreateAlignedTimeseries(
		devicePath,
		measurements,
		types,
		encodings,
		compressions,
		nil)

	// 忽略"已存在"错误
	if err != nil && !strings.Contains(err.Error(), "already exist") {
		return err
	}

	// 记录已创建的时间序列
	db.timeseriesCreationMutex.Lock()
	if db.createdTimeseries == nil {
		db.createdTimeseries = make(map[string]bool)
	}
	db.createdTimeseries[cacheKey] = true
	db.timeseriesCreationMutex.Unlock()

	return nil
}

// 修改后的查询方法 - 适配新的对齐时间序列结构
func (db *IoTDB) QueryByDeviceAndTimeRange(ctx context.Context, jobId string, deviceID string, start, end time.Time) ([]models.SensorData, error) {
	session, err := db.sessionPool.GetSession()
	if err != nil {
		return nil, err
	}
	defer db.sessionPool.PutBack(session)

	// 适配新的路径结构: root.{factory_id}.{device_id}
	// 由于设备ID可能在多个工厂中，我们需要查询所有工厂
	sql := fmt.Sprintf(`
        SELECT temperature, humidity, pressure, voltage, current, power, rpm, status, error_code, production_count
        FROM root.*.%s
        WHERE time >= %d AND time <= %d AND job_id = '%s'
        ORDER BY time
    `, deviceID, start.UnixMilli(), end.UnixMilli(), jobId)

	fmt.Printf(sql)
	var timeout int64 = 60000 // 60秒超时
	sessionDataSet, err := session.ExecuteQueryStatement(sql, &timeout)
	if err != nil {
		return nil, err
	}
	defer sessionDataSet.Close()

	var data []models.SensorData

	for {
		hasNext, err := sessionDataSet.Next()
		if err != nil {
			return nil, fmt.Errorf("QueryByDeviceAndTimeRange error reading next record: %w", err)
		}
		if !hasNext {
			break
		}
		record, err := sessionDataSet.GetRowRecord()
		if err != nil {
			return nil, fmt.Errorf("error getting row record: %w", err)
		}
		if record == nil {
			continue
		}

		// 从路径中提取工厂ID
		columnNames := sessionDataSet.GetColumnNames()
		factoryID := ""
		if len(columnNames) > 0 {
			pathParts := strings.Split(columnNames[0], ".")
			if len(pathParts) >= 2 {
				factoryID = pathParts[1] // root.factory_id.device_id.measurement
			}
		}

		sensorData := models.SensorData{
			Timestamp: time.UnixMilli(record.GetTimestamp()),
			FactoryID: factoryID,
			DeviceID:  deviceID,
		}

		// 从记录中提取传感器数据
		for i, field := range record.GetFields() {
			if field != nil && !field.IsNull() {
				columnName := ""
				if i < len(columnNames) {
					columnName = strings.ToLower(columnNames[i])
				}

				// 根据列名设置对应字段
				if strings.Contains(columnName, "temperature") {
					sensorData.Temperature = field.GetFloat32()
				} else if strings.Contains(columnName, "humidity") {
					sensorData.Humidity = field.GetFloat32()
				} else if strings.Contains(columnName, "pressure") {
					sensorData.Pressure = field.GetFloat32()
				} else if strings.Contains(columnName, "voltage") {
					sensorData.Voltage = field.GetFloat32()
				} else if strings.Contains(columnName, "current") {
					sensorData.Current = field.GetFloat32()
				} else if strings.Contains(columnName, "power") {
					sensorData.Power = field.GetFloat32()
				} else if strings.Contains(columnName, "rpm") {
					sensorData.RPM = field.GetInt64()
				} else if strings.Contains(columnName, "status") {
					sensorData.Status = field.GetText()
				} else if strings.Contains(columnName, "error_code") {
					sensorData.ErrorCode = int32(field.GetInt32())
				} else if strings.Contains(columnName, "production_count") {
					sensorData.ProductionCount = field.GetInt64()
				}
			}
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
	defer db.sessionPool.PutBack(session)

	var aggFunc string
	switch aggType {
	case "avg":
		aggFunc = "avg"
	case "max":
		aggFunc = "max_value"
	case "min":
		aggFunc = "min_value"
	default:
		aggFunc = "avg"
	}

	// 适配新的对齐时间序列路径
	sql := fmt.Sprintf(`
        SELECT %s(temperature)
        FROM root.*.%s
        WHERE time >= %d AND time <= %d AND job_id = '%s' GROUP BY ([%d, %d), 1m)
    `, aggFunc, deviceID, start.UnixMilli(), end.UnixMilli(), jobId, start.UnixMilli(), end.UnixMilli())

	fmt.Printf(sql)
	var timeout int64 = 60000
	sessionDataSet, err := session.ExecuteQueryStatement(sql, &timeout)
	if err != nil {
		fmt.Errorf("QueryAggregation ExecuteQueryStatement error executing statement: %w", err)
		return 0, err
	}
	defer sessionDataSet.Close()

	dataSize := 0
	hasNext, err := sessionDataSet.Next()
	if err != nil {
		fmt.Errorf("QueryAggregation Next error executing statement: %w", err)
		return 0, err
	}

	if hasNext {
		dataSize++
		record, err := sessionDataSet.GetRowRecord()
		if err != nil {
			fmt.Errorf("QueryAggregation GetRowRecord error executing statement: %w", err)
			return 0, err
		}
		if record != nil && len(record.GetFields()) > 0 {
			fmt.Println("QueryAggregation result field len:", len(record.GetFields()))
		}
	}
	fmt.Println("QueryAggregation dataSize:" + strconv.Itoa(dataSize))

	return 0, nil
}

func (db *IoTDB) QueryTimeRange(ctx context.Context, jobId string, start, end time.Time, limit int) ([]models.SensorData, error) {
	session, err := db.sessionPool.GetSession()
	if err != nil {
		return nil, err
	}
	defer db.sessionPool.PutBack(session)

	// 适配新的对齐时间序列路径
	sql := fmt.Sprintf(`
        SELECT temperature
        FROM root.**
        WHERE time >= %d AND time <= %d AND job_id = '%s'
        LIMIT %d
    `, start.UnixMilli(), end.UnixMilli(), jobId, limit)

	fmt.Printf(sql)
	var timeout int64 = 60000
	sessionDataSet, err := session.ExecuteQueryStatement(sql, &timeout)
	if err != nil {
		return nil, err
	}
	defer sessionDataSet.Close()

	var data []models.SensorData
	// 用于跟踪已处理的时间戳和设备
	processed := make(map[string]bool)

	for next, err := sessionDataSet.Next(); err == nil && next; next, err = sessionDataSet.Next() {
		record, err := sessionDataSet.GetRowRecord()
		if err != nil {
			return nil, fmt.Errorf("error getting row record: %w", err)
		}
		if record == nil {
			continue
		}

		// 分析第一个列名来获取工厂ID和设备ID
		columnNames := sessionDataSet.GetColumnNames()
		if len(columnNames) == 0 {
			continue
		}

		// 从路径中提取工厂ID和设备ID
		pathParts := strings.Split(columnNames[0], ".")
		if len(pathParts) < 4 { // root.factoryID.deviceID.measurement
			continue
		}

		factoryID := pathParts[1]
		deviceID := pathParts[2]
		timestamp := record.GetTimestamp()

		// 跳过已处理过的时间戳+设备组合
		key := fmt.Sprintf("%d-%s-%s", timestamp, factoryID, deviceID)
		if processed[key] {
			continue
		}
		processed[key] = true

		// 如果达到限制，提前退出
		if len(data) >= limit {
			break
		}
	}
	fmt.Println("QueryTimeRange data size:", len(data))

	return data, nil
}

func (db *IoTDB) QueryGroupBy(ctx context.Context, jobId string, start, end time.Time, groupBy string, interval time.Duration) (map[string]float32, error) {
	session, err := db.sessionPool.GetSession()
	if err != nil {
		return nil, err
	}
	defer db.sessionPool.PutBack(session)

	var sql string

	switch groupBy {
	case "status":
		// 按设备分组
		sql = fmt.Sprintf(`
            SELECT avg(temperature) AS avg_temp 
            FROM root.**
            WHERE time >= %d AND time <= %d and job_id = '%s'
            GROUP BY LEVEL=3
        `, start.UnixMilli(), end.UnixMilli(), jobId)
	case "factory":
		// 按工厂分组
		sql = fmt.Sprintf(`
            SELECT avg(temperature) AS avg_temp 
            FROM root.**
            WHERE time >= %d AND time <= %d and job_id = '%s'
            GROUP BY LEVEL=2
        `, start.UnixMilli(), end.UnixMilli(), jobId)
	case "time":
		// 按时间间隔分组
		intervalStr := formatInterval(interval)
		sql = fmt.Sprintf(`
            SELECT avg(temperature) AS avg_temp 
            FROM root.**
            WHERE time >= %d AND time <= %d AND job_id = '%s'
            GROUP BY ([%d, %d), %s)
        `, start.UnixMilli(), end.UnixMilli(), jobId, start.UnixMilli(), end.UnixMilli(), intervalStr)
	default:
		return nil, fmt.Errorf("unsupported groupBy type: %s", groupBy)
	}

	var timeout int64 = 60000
	fmt.Printf(sql)
	sessionDataSet, err := session.ExecuteQueryStatement(sql, &timeout)
	if err != nil {
		return nil, err
	}
	defer sessionDataSet.Close()

	result := make(map[string]float32)
	for {
		hasNext, err := sessionDataSet.Next()
		if err != nil {
			return nil, fmt.Errorf("QueryGroupBy error reading next record: %w", err)
		}
		if !hasNext {
			break
		}
		record, err := sessionDataSet.GetRowRecord()
		if err != nil {
			return nil, fmt.Errorf("error getting row record: %w", err)
		}
		if record == nil {
			continue
		}

		// 确定结果键名
		var key string
		if groupBy == "time" {
			// 对于按时间分组，使用时间戳作为键
			key = time.UnixMilli(record.GetTimestamp()).Format(time.RFC3339)
		} else {
			// 对于按设备或工厂分组，使用列名中的设备或工厂ID
			columnNames := sessionDataSet.GetColumnNames()
			if len(columnNames) > 0 {
				pathParts := strings.Split(columnNames[0], ".")
				if groupBy == "device" && len(pathParts) >= 3 {
					key = pathParts[2] // 设备ID
				} else if groupBy == "factory" && len(pathParts) >= 2 {
					key = pathParts[1] // 工厂ID
				} else {
					key = columnNames[0]
				}
			} else {
				key = fmt.Sprintf("unknown_%d", len(result))
			}
		}

		// 获取聚合值
		if len(record.GetFields()) > 0 {
			field := record.GetFields()[0]
			if !field.IsNull() {
				if field.GetDataType() == client.FLOAT {
					result[key] = field.GetFloat32()
				} else if field.GetDataType() == client.DOUBLE {
					result[key] = float32(field.GetFloat64())
				} else if field.GetDataType() == client.INT32 {
					result[key] = float32(field.GetInt32())
				} else if field.GetDataType() == client.INT64 {
					result[key] = float32(field.GetInt64())
				}
			}
		}
	}

	fmt.Println("QueryGroupBy result size:", len(result))
	return result, nil
}

func (db *IoTDB) Ping(ctx context.Context) error {
	session, err := db.sessionPool.GetSession()
	if err != nil {
		return err
	}
	defer db.sessionPool.PutBack(session)

	timeout := int64(10000)
	sessionDataSet, err := session.ExecuteQueryStatement("SHOW STORAGE GROUP", &timeout)
	if err != nil {
		return err
	}
	defer sessionDataSet.Close()
	return nil
}

// 格式化时间间隔，用于GROUP BY子句
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

// 从路径中解析设备信息
func parseDeviceInfo(columnName string) (factoryID, deviceID string) {
	parts := strings.Split(columnName, ".")
	if len(parts) >= 3 {
		factoryID = parts[1] // root.factory_id.device_id.measurement
		deviceID = parts[2]
	}
	return
}

// 提取分组键
func extractGroupKey(columnName, groupBy string) string {
	parts := strings.Split(columnName, ".")

	switch groupBy {
	case "factory":
		if len(parts) >= 2 {
			return parts[1] // root.factory_id.device_id.measurement
		}
	case "device":
		if len(parts) >= 3 {
			return parts[2] // root.factory_id.device_id.measurement
		}
	}

	return columnName
}

// 从记录中获取浮点值
func getIotFloatValue(field *client.Field) float32 {
	if field == nil || field.IsNull() {
		return 0
	}

	switch field.GetDataType() {
	case client.FLOAT:
		return field.GetFloat32()
	case client.DOUBLE:
		return float32(field.GetFloat64())
	case client.INT32:
		return float32(field.GetInt32())
	case client.INT64:
		return float32(field.GetInt64())
	default:
		// 尝试文本解析
		if text := field.GetText(); text != "" {
			if val, err := strconv.ParseFloat(text, 32); err == nil {
				return float32(val)
			}
		}
		return 0
	}
}

// 从记录中获取整数值
func getInt64Value(field *client.Field) int64 {
	if field == nil || field.IsNull() {
		return 0
	}

	switch field.GetDataType() {
	case client.INT64:
		return field.GetInt64()
	case client.INT32:
		return int64(field.GetInt32())
	case client.FLOAT:
		return int64(field.GetFloat32())
	case client.DOUBLE:
		return int64(field.GetFloat64())
	default:
		// 尝试文本解析
		if text := field.GetText(); text != "" {
			if val, err := strconv.ParseInt(text, 10, 64); err == nil {
				return val
			}
		}
		return 0
	}
}

// 从记录中获取文本值
func getTextValue(field *client.Field) string {
	if field == nil || field.IsNull() {
		return ""
	}

	if field.GetDataType() == client.TEXT {
		return field.GetText()
	}

	// 尝试将其他类型转换为文本
	switch field.GetDataType() {
	case client.FLOAT:
		return fmt.Sprintf("%f", field.GetFloat32())
	case client.DOUBLE:
		return fmt.Sprintf("%f", field.GetFloat64())
	case client.INT32:
		return strconv.Itoa(int(field.GetInt32()))
	case client.INT64:
		return strconv.FormatInt(field.GetInt64(), 10)
	default:
		if val := field.GetValue(); val != nil {
			return fmt.Sprintf("%v", val)
		}
		return ""
	}
}

// 错误检查函数，参考官方示例
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

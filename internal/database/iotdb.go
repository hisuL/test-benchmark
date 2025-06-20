package database

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iotdb-client-go/client"
	"test-benchmark/internal/models"
)

type IoTDB struct {
	session  client.Session
	host     string
	port     int
	username string
	password string
}

func NewIoTDB(host string, port int, username, password string) *IoTDB {
	return &IoTDB{
		host:     host,
		port:     port,
		username: username,
		password: password,
	}
}

func (db *IoTDB) Name() string {
	return "IoTDB"
}

func (db *IoTDB) Connect(ctx context.Context) error {
	// 检查session是否已初始化（假设Config字段可以用来判断）
	// 这里需要根据client.Session的具体结构来判断
	if db.session.GetSessionId() != 0 { // 假设有这样的方法
		// 验证连接是否仍然有效
		if err := db.Ping(ctx); err == nil {
			return nil
		}
	}

	config := &client.Config{
		Host:     db.host,
		Port:     strconv.Itoa(db.port),
		UserName: db.username,
		Password: db.password,
	}

	db.session = client.NewSession(config)

	err := db.session.Open(false, 0)
	if err != nil {
		return err
	}

	return db.Ping(ctx)
}

func (db *IoTDB) Close() error {
	return db.session.Close()
}

func (db *IoTDB) CreateSchema(ctx context.Context) error {
	storageGroups := []string{
		"CREATE DATABASE root.benchmark",
	}

	for _, sg := range storageGroups {
		_, err := db.session.ExecuteNonQueryStatement(sg)
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}

	return nil
}

func (db *IoTDB) DropSchema(ctx context.Context) error {
	_, err := db.session.ExecuteNonQueryStatement("DELETE DATABASE root.benchmark.*")
	return err
}

func (db *IoTDB) WriteBatch(ctx context.Context, data []models.SensorData) error {
	if len(data) == 0 {
		return nil
	}

	// Group data by device
	deviceGroups := make(map[string][]models.SensorData)
	for _, record := range data {
		key := fmt.Sprintf("%s_%s", record.FactoryID, record.DeviceID)
		deviceGroups[key] = append(deviceGroups[key], record)
	}

	for deviceKey, records := range deviceGroups {
		devicePath := fmt.Sprintf("root.benchmark.%s", strings.ReplaceAll(deviceKey, "-", "_"))

		var deviceIds []string
		var timestamps []int64
		var measurementsList [][]string
		var valuesList [][]interface{}
		var typesList [][]client.TSDataType

		for _, record := range records {
			deviceIds = append(deviceIds, devicePath)
			timestamps = append(timestamps, record.Timestamp.UnixMilli())

			measurements := []string{
				"temperature", "humidity", "pressure", "voltage",
				"current", "power", "rpm", "status", "error_code", "production_count",
			}
			values := []interface{}{
				record.Temperature, record.Humidity, record.Pressure, record.Voltage,
				record.Current, record.Power, record.RPM, record.Status,
				record.ErrorCode, record.ProductionCount,
			}
			types := []client.TSDataType{
				client.FLOAT, client.FLOAT, client.FLOAT, client.FLOAT,
				client.FLOAT, client.FLOAT, client.INT64, client.TEXT,
				client.INT32, client.INT64,
			}

			measurementsList = append(measurementsList, measurements)
			valuesList = append(valuesList, values)
			typesList = append(typesList, types)
		}

		_, err := db.session.InsertRecords(deviceIds, measurementsList, typesList, valuesList, timestamps)

		if err != nil {
			return fmt.Errorf("failed to insert batch data for device %s: %w", deviceKey, err)
		}
	}

	return nil
}

func (db *IoTDB) QueryByDeviceAndTimeRange(ctx context.Context, deviceID string, start, end time.Time) ([]models.SensorData, error) {
	devicePath := fmt.Sprintf("root.benchmark.*.%s", deviceID)

	sql := fmt.Sprintf(`
        SELECT temperature, humidity, pressure, voltage, current, power, rpm, status, error_code, production_count
        FROM %s
        WHERE time >= %d AND time <= %d
        ORDER BY time
    `, devicePath, start.UnixMilli(), end.UnixMilli())

	sessionDataSet, err := db.session.ExecuteQueryStatement(sql, nil)
	if err != nil {
		return nil, err
	}
	defer sessionDataSet.Close()

	var data []models.SensorData

	for {
		hasNext, err := sessionDataSet.Next()
		if err != nil {
			return nil, fmt.Errorf("error reading next record: %w", err)
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

		// 使用索引方式获取（如果字段顺序固定）
		/*		sensorData := models.SensorData{
					Timestamp:       time.UnixMilli(record.GetTimestamp()),
					DeviceID:        deviceID,
					Temperature:     getFloatFromRecord(record, 0),
					Humidity:        getFloatFromRecord(record, 1),
					Pressure:        getFloatFromRecord(record, 2),
					Voltage:         getFloatFromRecord(record, 3),
					Current:         getFloatFromRecord(record, 4),
					Power:           getFloatFromRecord(record, 5),
					RPM:             getInt64FromRecord(record, 6),
					Status:          getStringFromRecord(record, 7),
					ErrorCode:       int32(getInt64FromRecord(record, 8)),
					ProductionCount: getInt64FromRecord(record, 9),
				}
		*/
		// 或者使用字段名方式获取（如果担心索引不匹配）
		sensorData := models.SensorData{
			Timestamp:       time.UnixMilli(record.GetTimestamp()),
			DeviceID:        deviceID,
			Temperature:     getFloatFromRecordByName(record, "temperature"),
			Humidity:        getFloatFromRecordByName(record, "humidity"),
			Pressure:        getFloatFromRecordByName(record, "pressure"),
			Voltage:         getFloatFromRecordByName(record, "voltage"),
			Current:         getFloatFromRecordByName(record, "current"),
			Power:           getFloatFromRecordByName(record, "power"),
			RPM:             getInt64FromRecordByName(record, "rpm"),
			Status:          getStringFromRecordByName(record, "status"),
			ErrorCode:       int32(getInt64FromRecordByName(record, "error_code")),
			ProductionCount: getInt64FromRecordByName(record, "production_count"),
		}

		data = append(data, sensorData)
	}

	return data, nil
}

func (db *IoTDB) QueryAggregation(ctx context.Context, deviceID string, start, end time.Time, aggType string) (float32, error) {
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

	// IoTDB 1.3.0 正确语法
	sql := fmt.Sprintf(`
        SELECT %s(temperature)
        FROM root.benchmark.*.%s
        WHERE time >= %d AND time <= %d
    `, aggFunc, deviceID, start.UnixMilli(), end.UnixMilli())

	sessionDataSet, err := db.session.ExecuteQueryStatement(sql, nil)
	if err != nil {
		return 0, err
	}
	defer sessionDataSet.Close()

	hasNext, err := sessionDataSet.Next()
	if err != nil {
		return 0, err
	}

	if hasNext {
		record, err := sessionDataSet.GetRowRecord()
		if err != nil {
			return 0, err
		}
		if record != nil && len(record.GetFields()) > 0 {
			return getFloatFromRecord(record, 0), nil
		}
	}

	return 0, nil
}

func (db *IoTDB) QueryTimeRange(ctx context.Context, start, end time.Time, limit int) ([]models.SensorData, error) {
	// IoTDB 1.3.0 正确语法
	sql := fmt.Sprintf(`
        SELECT ** 
        FROM root.benchmark
        WHERE time >= %d AND time <= %d
        ORDER BY time ASC
        LIMIT %d
    `, start.UnixMilli(), end.UnixMilli(), limit)

	sessionDataSet, err := db.session.ExecuteQueryStatement(sql, nil)
	if err != nil {
		return nil, err
	}
	defer sessionDataSet.Close()

	var data []models.SensorData

	for {
		hasNext, err := sessionDataSet.Next()
		if err != nil {
			return nil, fmt.Errorf("error reading next record: %w", err)
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

		// 从列名中解析设备和工厂信息
		columnNames := sessionDataSet.GetColumnNames()
		factoryID, deviceID := "", ""
		if len(columnNames) > 0 {
			factoryID, deviceID = parseDeviceInfo(columnNames[0])
		}

		sensorData := models.SensorData{
			Timestamp:       time.UnixMilli(record.GetTimestamp()),
			FactoryID:       factoryID,
			DeviceID:        deviceID,
			Temperature:     getFloatFromRecord(record, 0),
			Humidity:        getFloatFromRecord(record, 1),
			Pressure:        getFloatFromRecord(record, 2),
			Voltage:         getFloatFromRecord(record, 3),
			Current:         getFloatFromRecord(record, 4),
			Power:           getFloatFromRecord(record, 5),
			RPM:             getInt64FromRecord(record, 6),
			Status:          getStringFromRecord(record, 7),
			ErrorCode:       int32(getInt64FromRecord(record, 8)),
			ProductionCount: getInt64FromRecord(record, 9),
		}

		data = append(data, sensorData)
	}

	return data, nil
}

func (db *IoTDB) QueryGroupBy(ctx context.Context, start, end time.Time, groupBy string, interval time.Duration) (map[string]float32, error) {
	var sql string

	switch groupBy {
	case "device":
		// 按设备分组 - 使用GROUP BY LEVEL
		sql = fmt.Sprintf(`
            SELECT avg(temperature)
            FROM root.benchmark.**
            WHERE time >= %d AND time <= %d
            GROUP BY LEVEL = 3
        `, start.UnixMilli(), end.UnixMilli())

	case "factory":
		// 按工厂分组 - 使用GROUP BY LEVEL
		sql = fmt.Sprintf(`
            SELECT avg(temperature)
            FROM root.benchmark.**
            WHERE time >= %d AND time <= %d
            GROUP BY LEVEL = 2
        `, start.UnixMilli(), end.UnixMilli())

	case "time":
		// 按时间分组 - 使用GROUP BY TIME
		intervalStr := formatInterval(interval)
		sql = fmt.Sprintf(`
            SELECT avg(temperature)
            FROM root.benchmark.**
            WHERE time >= %d AND time <= %d
            GROUP BY (%s, [%d, %d))
        `, start.UnixMilli(), end.UnixMilli(), intervalStr, start.UnixMilli(), end.UnixMilli())

	default:
		return nil, fmt.Errorf("unsupported groupBy type: %s", groupBy)
	}

	sessionDataSet, err := db.session.ExecuteQueryStatement(sql, nil)
	if err != nil {
		return nil, err
	}
	defer sessionDataSet.Close()

	result := make(map[string]float32)
	for {
		hasNext, err := sessionDataSet.Next()
		if err != nil {
			return nil, fmt.Errorf("error reading next record: %w", err)
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

		var key string
		switch groupBy {
		case "device", "factory":
			// 从列名中提取分组信息
			columnNames := sessionDataSet.GetColumnNames()
			if len(columnNames) > 0 {
				key = extractGroupKey(columnNames[0], groupBy)
			} else {
				key = fmt.Sprintf("unknown_%d", len(result))
			}
		case "time":
			// 使用时间戳作为key
			key = time.UnixMilli(record.GetTimestamp()).Format("2006-01-02 15:04:05")
		}

		if len(record.GetFields()) > 0 {
			result[key] = getFloatFromRecord(record, 0)
		}
	}

	return result, nil
}

// 解析设备信息
func parseDeviceInfo(columnName string) (factoryID, deviceID string) {
	// 假设列名格式：root.benchmark.factory_001.device_050.temperature
	parts := strings.Split(columnName, ".")
	if len(parts) >= 5 {
		factoryID = parts[2] // factory_001
		deviceID = parts[3]  // device_050
	}
	return
}

// 提取分组键
func extractGroupKey(columnName, groupBy string) string {
	parts := strings.Split(columnName, ".")

	switch groupBy {
	case "factory":
		if len(parts) >= 3 {
			return parts[2] // factory_001
		}
	case "device":
		if len(parts) >= 4 {
			return fmt.Sprintf("%s.%s", parts[2], parts[3]) // factory_001.device_050
		}
	}

	return columnName
}

// 格式化时间间隔
func formatInterval(interval time.Duration) string {
	if interval >= time.Hour {
		hours := int(interval.Hours())
		return fmt.Sprintf("%dh", hours)
	} else if interval >= time.Minute {
		minutes := int(interval.Minutes())
		return fmt.Sprintf("%dm", minutes)
	} else {
		seconds := int(interval.Seconds())
		return fmt.Sprintf("%ds", seconds)
	}
}

func (db *IoTDB) Ping(ctx context.Context) error {
	sessionDataSet, err := db.session.ExecuteQueryStatement("SHOW DATABASES", nil)
	if err != nil {
		return err
	}
	defer sessionDataSet.Close()
	return nil
}

// 修正后的 Helper functions - 基于实际的 Field API
func getFloatFromRecord(record *client.RowRecord, index int) float32 {
	fields := record.GetFields()
	if index >= len(fields) {
		return 0
	}

	field := fields[index]
	if field == nil || field.IsNull() {
		return 0
	}

	// 根据数据类型使用对应的方法
	switch field.GetDataType() {
	case client.DOUBLE:
		return float32(field.GetFloat64())
	case client.FLOAT:
		return field.GetFloat32()
	case client.INT64:
		return float32(field.GetInt64())
	case client.INT32:
		return float32(field.GetInt32())
	default:
		// 尝试通过 GetValue() 和类型断言
		if val := field.GetValue(); val != nil {
			switch v := val.(type) {
			case float32:
				return v
			case float64:
				return float32(v)
			case int64:
				return float32(v)
			case int32:
				return float32(v)
			case int:
				return float32(v)
			default:
				// 最后尝试字符串转换
				if str := field.GetText(); str != "" {
					if f, err := strconv.ParseFloat(str, 32); err == nil {
						return float32(f)
					}
				}
			}
		}
	}
	return 0
}

func getInt64FromRecord(record *client.RowRecord, index int) int64 {
	fields := record.GetFields()
	if index >= len(fields) {
		return 0
	}

	field := fields[index]
	if field == nil || field.IsNull() {
		return 0
	}

	// 根据数据类型使用对应的方法
	switch field.GetDataType() {
	case client.INT64:
		return field.GetInt64()
	case client.INT32:
		return int64(field.GetInt32())
	case client.DOUBLE:
		return int64(field.GetFloat64())
	case client.FLOAT:
		return int64(field.GetFloat32())
	default:
		// 尝试通过 GetValue() 和类型断言
		if val := field.GetValue(); val != nil {
			switch v := val.(type) {
			case int64:
				return v
			case int32:
				return int64(v)
			case int:
				return int64(v)
			case float64:
				return int64(v)
			case float32:
				return int64(v)
			default:
				// 最后尝试字符串转换
				if str := field.GetText(); str != "" {
					if i, err := strconv.ParseInt(str, 10, 64); err == nil {
						return i
					}
				}
			}
		}
	}
	return 0
}

func getStringFromRecord(record *client.RowRecord, index int) string {
	fields := record.GetFields()
	if index >= len(fields) {
		return ""
	}

	field := fields[index]
	if field == nil || field.IsNull() {
		return ""
	}

	// 对于文本类型，直接使用 GetText()
	if field.GetDataType() == client.TEXT || field.GetDataType() == client.STRING {
		return field.GetText()
	}

	// 对于其他类型，也尝试 GetText()
	if text := field.GetText(); text != "" {
		return text
	}

	// 最后通过 GetValue() 和类型断言
	if val := field.GetValue(); val != nil {
		switch v := val.(type) {
		case string:
			return v
		case []byte:
			return string(v)
		default:
			return fmt.Sprintf("%v", v)
		}
	}

	return ""
}

// 根据字段名获取浮点数值（作为备用方案）
func getFloatFromRecordByName(record *client.RowRecord, fieldName string) float32 {
	fields := record.GetFields()

	for _, field := range fields {
		if field == nil || field.IsNull() {
			continue
		}

		// 检查字段名是否匹配
		if strings.Contains(strings.ToLower(field.GetName()), strings.ToLower(fieldName)) {
			switch field.GetDataType() {
			case client.DOUBLE:
				return float32(field.GetFloat64())
			case client.FLOAT:
				return field.GetFloat32()
			case client.INT64:
				return float32(field.GetInt64())
			case client.INT32:
				return float32(field.GetInt32())
			}
		}
	}
	return 0
}

// 根据字段名获取整数值（作为备用方案）
func getInt64FromRecordByName(record *client.RowRecord, fieldName string) int64 {
	fields := record.GetFields()

	for _, field := range fields {
		if field == nil || field.IsNull() {
			continue
		}

		// 检查字段名是否匹配
		if strings.Contains(strings.ToLower(field.GetName()), strings.ToLower(fieldName)) {
			switch field.GetDataType() {
			case client.INT64:
				return field.GetInt64()
			case client.INT32:
				return int64(field.GetInt32())
			case client.DOUBLE:
				return int64(field.GetFloat64())
			case client.FLOAT:
				return int64(field.GetFloat32())
			}
		}
	}
	return 0
}

// 根据字段名获取字符串值（作为备用方案）
func getStringFromRecordByName(record *client.RowRecord, fieldName string) string {
	fields := record.GetFields()

	for _, field := range fields {
		if field == nil || field.IsNull() {
			continue
		}

		// 检查字段名是否匹配
		if strings.Contains(strings.ToLower(field.GetName()), strings.ToLower(fieldName)) {
			return field.GetText()
		}
	}
	return ""
}

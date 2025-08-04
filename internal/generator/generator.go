package generator

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"golang.org/x/net/context"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"

	"test-benchmark/internal/config"
	"test-benchmark/internal/models"
)

type DataGenerator struct {
	config *config.DataGenConfig
	rand   *rand.Rand
	// 移除 allEvents 字段，改为存储元数据
	totalRecords   int             // 总记录数
	jobStartTimes  []time.Time     // Job启动时间
	jobAssignments []JobAssignment // Job分配信息
	recordsPerJob  int             // 每个Job的记录数
	dayStart       time.Time       // 一天的开始时间
}

// 添加批次大小常量
const batchSize = 10000

func NewDataGenerator(cfg *config.DataGenConfig) *DataGenerator {
	return &DataGenerator{
		config: cfg,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// 初始化元数据（只计算，不存储所有事件）
func (g *DataGenerator) initializeMetadata() error {
	if g.totalRecords > 0 {
		return nil // 已经初始化过了
	}

	// 解析日期
	dayStart, err := ParseDay(g.config.Day)
	if err != nil {
		return fmt.Errorf("解析日期失败: %v", err)
	}
	g.dayStart = dayStart

	// 计算基础参数
	g.recordsPerJob = g.calculateRecordsPerJob()
	g.totalRecords = g.config.JobCount * g.recordsPerJob
	g.jobStartTimes = g.calculateJobStartTimes(dayStart)
	g.jobAssignments = g.assignJobsToDevices()

	g.logf("总共将生成 %d 条记录 (JobCount: %d, RecordsPerJob: %d)\n",
		g.totalRecords, g.config.JobCount, g.recordsPerJob)

	return nil
}

// 原有的 GenerateData 方法保持不变（用于小数据量）
func (g *DataGenerator) GenerateData() ([]models.SensorData, error) {
	// 对于小数据量，仍然可以一次性生成
	if err := g.initializeMetadata(); err != nil {
		return nil, err
	}

	// 如果数据量太大，建议使用批次方法
	if g.totalRecords > 20000000 { // 超过100万条记录建议使用批次方法
		return nil, fmt.Errorf("数据量过大 (%d 条记录)，建议使用 GenerateDataBatch 方法", g.totalRecords)
	}

	events := g.generateBatchEvents(0, g.totalRecords)
	data := make([]models.SensorData, 0, len(events))

	for _, event := range events {
		assignment := g.jobAssignments[event.JobId-1]
		factoryName := fmt.Sprintf("factory_%03d", assignment.FactoryId)
		deviceName := fmt.Sprintf("device_%03d", assignment.DeviceId)
		jobName := fmt.Sprintf("job_%06d", event.JobId)

		record := g.generateSensorRecord(factoryName, jobName, deviceName, event.Time)
		data = append(data, record)
	}

	return data, nil
}

// JobSchedule 表示Job的调度信息
type JobSchedule struct {
	JobId     int
	StartTime time.Time
	EndTime   time.Time
}

// DataCollectionEvent 表示一个数据采集事件
type DataCollectionEvent struct {
	Time  time.Time
	JobId int
}

type JobAssignment struct {
	JobId     int
	FactoryId int
	DeviceId  int
}

// parseDay 解析日期字符串为时间对象
func ParseDay(dayStr string) (time.Time, error) {
	// 解析 YYYY-MM-DD 格式的日期
	parsedTime, err := time.Parse("2006-01-02", dayStr)
	if err != nil {
		return time.Time{}, err
	}

	// 返回当天的0点时间（UTC）
	return time.Date(parsedTime.Year(), parsedTime.Month(), parsedTime.Day(),
		0, 0, 0, 0, time.UTC), nil
}

// assignJobsToDevices 为每个Job分配固定的Factory和Device
func (g *DataGenerator) assignJobsToDevices() []JobAssignment {
	assignments := make([]JobAssignment, g.config.JobCount)

	// 计算总的设备数量
	totalDevices := g.config.FactoryCount * g.config.DeviceCount

	for jobId := 1; jobId <= g.config.JobCount; jobId++ {
		// 使用轮询方式分配设备，确保均匀分布
		deviceIndex := (jobId - 1) % totalDevices

		// 计算对应的factory和device
		factoryId := deviceIndex/g.config.DeviceCount + 1
		deviceId := deviceIndex%g.config.DeviceCount + 1

		assignments[jobId-1] = JobAssignment{
			JobId:     jobId,
			FactoryId: factoryId,
			DeviceId:  deviceId,
		}
	}

	return assignments
}

// 添加静态方法用于计算Job分配
func CalculateJobAssignment(jobId, factoryCount, deviceCount int) JobAssignment {
	totalDevices := factoryCount * deviceCount
	deviceIndex := (jobId - 1) % totalDevices

	factoryId := deviceIndex/deviceCount + 1
	deviceId := deviceIndex%deviceCount + 1

	return JobAssignment{
		JobId:     jobId,
		FactoryId: factoryId,
		DeviceId:  deviceId,
	}
}

// 通过JobID获取FactoryID和DeviceID的便捷方法
func GetDeviceByJobId(jobId, factoryCount, deviceCount int) (factoryId, deviceId int) {
	assignment := CalculateJobAssignment(jobId, factoryCount, deviceCount)
	return assignment.FactoryId, assignment.DeviceId
}

// 验证JobID是否有效
func IsValidJobId(jobId, jobCount int) bool {
	return jobId >= 1 && jobId <= jobCount
}

// shouldCollectData 检查在指定时间是否应该采集该job的数据
func (g *DataGenerator) shouldCollectData(currentTime time.Time, schedule JobSchedule) bool {
	// 检查时间是否在job运行期间
	if currentTime.Before(schedule.StartTime) || currentTime.After(schedule.EndTime) {
		return false
	}

	// 检查是否是采集时间点
	elapsed := currentTime.Sub(schedule.StartTime)
	timeInterval := time.Duration(g.config.TimeInterval) * time.Second

	// 如果elapsed刚好是采集间隔的整数倍，则需要采集
	return elapsed%timeInterval == 0
}

// calculateJobStartTimes 计算每个job在24小时内的启动时间
func (g *DataGenerator) calculateJobStartTimes(dayStart time.Time) []time.Time {
	startTimes := make([]time.Time, g.config.JobCount)

	// 24小时 = 86400秒
	dayDuration := 24 * time.Hour

	// 计算job之间的时间间隔
	if g.config.JobCount == 1 {
		// 如果只有一个job，从0点开始
		startTimes[0] = dayStart
	} else {
		// 多个job平均分布在24小时内
		interval := dayDuration / time.Duration(g.config.JobCount)

		for i := 0; i < g.config.JobCount; i++ {
			startTimes[i] = dayStart.Add(time.Duration(i) * interval)
		}
	}

	return startTimes
}

// calculateRecordsPerJob 计算每个job运行期间会产生的记录数
func (g *DataGenerator) calculateRecordsPerJob() int {
	// job运行时间内，按照采集间隔会产生多少条记录
	// 记录数 = job运行时间 / 采集间隔
	return g.config.JobRunTime / g.config.TimeInterval
}

// generateJobData 为单个job生成运行期间的所有数据
func (g *DataGenerator) generateJobData(factoryName, jobName, deviceName string, startTime time.Time) []models.SensorData {
	recordsPerJob := g.calculateRecordsPerJob()
	jobData := make([]models.SensorData, 0, recordsPerJob)

	currentTime := startTime

	// 在job运行期间，按照时间间隔采集数据
	for i := 0; i < recordsPerJob; i++ {
		record := g.generateSensorRecord(factoryName, jobName, deviceName, currentTime)
		jobData = append(jobData, record)

		// 增加时间间隔
		currentTime = currentTime.Add(time.Duration(g.config.TimeInterval) * time.Second)
	}

	return jobData
}

func (g *DataGenerator) generateSensorRecord(factoryId, jobid string, deviceId string, timestamp time.Time) models.SensorData {
	// Generate realistic sensor data with some correlation
	baseTemp := 20.0 + g.rand.Float32()*30.0 // 20-50°C
	humidity := 30.0 + g.rand.Float32()*40.0 // 30-70%

	// Pressure correlated with temperature
	pressure := 1000.0 + (baseTemp-35.0)*2.0 + g.rand.Float32()*20.0 // around 1000-1040 hPa

	// Electrical parameters
	voltage := 220.0 + g.rand.Float32()*20.0 - 10.0 // 210-230V
	current := 5.0 + g.rand.Float32()*10.0          // 5-15A
	power := voltage * current                      // Power = V * I

	// Mechanical parameters
	rpm := int64(1000 + g.rand.Intn(2000)) // 1000-3000 RPM

	// Status and error simulation
	status := g.generateStatus()
	errorCode := g.generateErrorCode(status)

	// Production count (cumulative)
	productionCount := int64(timestamp.Unix()/3600) * int64(10+g.rand.Intn(50)) // 10-60 per hour

	return models.SensorData{
		Timestamp:       timestamp,
		FactoryID:       factoryId,
		JobId:           jobid,
		DeviceID:        deviceId,
		Temperature:     baseTemp,
		Humidity:        humidity,
		Pressure:        pressure,
		Voltage:         voltage,
		Current:         current,
		Power:           power,
		RPM:             rpm,
		Status:          status,
		ErrorCode:       errorCode,
		ProductionCount: productionCount,
	}
}

func (g *DataGenerator) generateStatus() string {
	statuses := []string{"running", "idle", "maintenance", "error", "stopped"}
	weights := []int{70, 15, 5, 5, 5} // running is most common

	totalWeight := 0
	for _, w := range weights {
		totalWeight += w
	}

	r := g.rand.Intn(totalWeight)
	currentWeight := 0

	for i, weight := range weights {
		currentWeight += weight
		if r < currentWeight {
			return statuses[i]
		}
	}

	return "running"
}

func (g *DataGenerator) generateErrorCode(status string) int32 {
	if status == "error" {
		return int32(1000 + g.rand.Intn(100)) // Error codes 1000-1099
	} else if status == "maintenance" {
		return int32(2000 + g.rand.Intn(10)) // Maintenance codes 2000-2009
	}
	return 0 // No error
}

// GenerateDataBatch 按需生成批次数据，不预存所有事件
func (g *DataGenerator) GenerateDataBatch(ctx context.Context, startOffset, batchSize int) ([]models.SensorData, bool, error) {
	// 初始化元数据
	if err := g.initializeMetadata(); err != nil {
		return nil, false, err
	}

	// 检查是否已经处理完所有数据
	if startOffset >= g.totalRecords {
		return nil, true, nil // 返回 true 表示已完成
	}

	endOffset := startOffset + batchSize
	if endOffset > g.totalRecords {
		endOffset = g.totalRecords
	}

	// 按需生成当前批次的事件
	batchEvents := g.generateBatchEvents(startOffset, endOffset)

	// 生成传感器数据
	batch := make([]models.SensorData, 0, len(batchEvents))
	for _, event := range batchEvents {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		default:
		}

		assignment := g.jobAssignments[event.JobId-1]
		factoryName := fmt.Sprintf("factory_%03d", assignment.FactoryId)
		deviceName := fmt.Sprintf("device_%03d", assignment.DeviceId)
		jobName := fmt.Sprintf("job_%06d", event.JobId)

		record := g.generateSensorRecord(factoryName, jobName, deviceName, event.Time)
		batch = append(batch, record)
	}

	isComplete := endOffset >= g.totalRecords
	g.logf("已生成批次 %d-%d/%d 条记录\n", startOffset+1, endOffset, g.totalRecords)

	return batch, isComplete, nil
}

// generateBatchEvents 按需生成指定范围的事件（按时间排序）
func (g *DataGenerator) generateBatchEvents(startOffset, endOffset int) []DataCollectionEvent {
	if endOffset > g.totalRecords {
		endOffset = g.totalRecords
	}

	batchSize := endOffset - startOffset
	events := make([]DataCollectionEvent, 0, batchSize)
	timeInterval := time.Duration(g.config.TimeInterval) * time.Second

	// 计算全局索引对应的事件
	for globalIndex := startOffset; globalIndex < endOffset; globalIndex++ {
		// 通过全局索引计算对应的JobId和该Job内的记录索引
		jobIndex := globalIndex / g.recordsPerJob    // 哪个Job
		recordIndex := globalIndex % g.recordsPerJob // Job内第几条记录

		jobId := jobIndex + 1
		jobStartTime := g.jobStartTimes[jobIndex]
		eventTime := jobStartTime.Add(time.Duration(recordIndex) * timeInterval)

		events = append(events, DataCollectionEvent{
			Time:  eventTime,
			JobId: jobId,
		})
	}

	// 对当前批次按时间排序
	sort.Slice(events, func(i, j int) bool {
		return events[i].Time.Before(events[j].Time)
	})

	return events
}

// 如果需要严格的全局时序，可以使用这个方法
func (g *DataGenerator) GenerateDataBatchWithGlobalOrder(ctx context.Context, startOffset, batchSize int) ([]models.SensorData, bool, error) {
	// 初始化元数据
	if err := g.initializeMetadata(); err != nil {
		return nil, false, err
	}

	// 检查是否已经处理完所有数据
	if startOffset >= g.totalRecords {
		return nil, true, nil
	}

	endOffset := startOffset + batchSize
	if endOffset > g.totalRecords {
		endOffset = g.totalRecords
	}

	// 生成全局排序的批次数据
	batch := g.generateGlobalOrderedBatch(ctx, startOffset, endOffset)

	isComplete := endOffset >= g.totalRecords
	g.logf("已生成全局排序批次 %d-%d/%d 条记录\n", startOffset+1, endOffset, g.totalRecords)

	return batch, isComplete, nil
}

// generateGlobalOrderedBatch 生成全局时间排序的批次
func (g *DataGenerator) generateGlobalOrderedBatch(ctx context.Context, startOffset, endOffset int) []models.SensorData {
	timeInterval := time.Duration(g.config.TimeInterval) * time.Second

	// 生成所有可能的时间点映射 (时间戳 -> 该时间点的所有事件)
	timeEventMap := make(map[int64][]DataCollectionEvent)

	// 只生成当前批次范围内的事件
	for globalIndex := startOffset; globalIndex < endOffset; globalIndex++ {
		jobIndex := globalIndex / g.recordsPerJob
		recordIndex := globalIndex % g.recordsPerJob

		jobId := jobIndex + 1
		jobStartTime := g.jobStartTimes[jobIndex]
		eventTime := jobStartTime.Add(time.Duration(recordIndex) * timeInterval)

		timestamp := eventTime.Unix()
		timeEventMap[timestamp] = append(timeEventMap[timestamp], DataCollectionEvent{
			Time:  eventTime,
			JobId: jobId,
		})
	}

	// 按时间戳排序
	timestamps := make([]int64, 0, len(timeEventMap))
	for ts := range timeEventMap {
		timestamps = append(timestamps, ts)
	}
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})

	// 按时间顺序生成数据
	batch := make([]models.SensorData, 0, endOffset-startOffset)
	for _, timestamp := range timestamps {
		for _, event := range timeEventMap[timestamp] {
			select {
			case <-ctx.Done():
				return batch
			default:
			}

			assignment := g.jobAssignments[event.JobId-1]
			factoryName := fmt.Sprintf("factory_%03d", assignment.FactoryId)
			deviceName := fmt.Sprintf("device_%03d", assignment.DeviceId)
			jobName := fmt.Sprintf("job_%06d", event.JobId)

			record := g.generateSensorRecord(factoryName, jobName, deviceName, event.Time)
			batch = append(batch, record)
		}
	}

	return batch
}

// writeBatchToBinary 将一批数据写入二进制文件
func writeBatchToBinary(writer *bufio.Writer, batch []models.SensorData) error {
	for _, record := range batch {
		// 写入时间戳
		if err := binary.Write(writer, binary.LittleEndian, record.Timestamp.Unix()); err != nil {
			return err
		}

		// 写入字符串长度和内容
		writeString := func(s string) error {
			length := uint16(len(s))
			if err := binary.Write(writer, binary.LittleEndian, length); err != nil {
				return err
			}
			_, err := writer.WriteString(s)
			return err
		}

		// 写入各个字段
		if err := writeString(record.FactoryID); err != nil {
			return err
		}
		if err := writeString(record.JobId); err != nil {
			return err
		}
		if err := writeString(record.DeviceID); err != nil {
			return err
		}

		// 写入数值字段
		if err := binary.Write(writer, binary.LittleEndian, record.Temperature); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.LittleEndian, record.Humidity); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.LittleEndian, record.Pressure); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.LittleEndian, record.Voltage); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.LittleEndian, record.Current); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.LittleEndian, record.Power); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.LittleEndian, record.RPM); err != nil {
			return err
		}
		if err := writeString(record.Status); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.LittleEndian, record.ErrorCode); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.LittleEndian, record.ProductionCount); err != nil {
			return err
		}
	}
	return nil
}

// GenerateDataToFile 生成数据并直接写入CSV文件，使用流式处理
func (g *DataGenerator) GenerateDataToFile(filename string) error {
	// 初始化元数据
	if err := g.initializeMetadata(); err != nil {
		return err
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("创建文件失败: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入CSV表头
	headers := []string{
		"timestamp", "factory_id", "job_id", "device_id",
		"temperature", "humidity", "pressure", "voltage",
		"current", "power", "rpm", "status", "error_code",
		"production_count",
	}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("写入表头失败: %v", err)
	}

	// 流式处理数据
	recordsWritten := 0
	batchCSV := make([][]string, 0, batchSize)

	for offset := 0; offset < g.totalRecords; offset += batchSize {
		// 使用批次方法生成数据
		batch, _, err := g.GenerateDataBatchWithGlobalOrder(context.Background(), offset, batchSize)
		if err != nil {
			return fmt.Errorf("生成数据批次失败: %v", err)
		}

		// 转换为CSV格式
		for _, record := range batch {
			csvRecord := []string{
				strconv.FormatInt(record.Timestamp.Unix(), 10),
				record.FactoryID,
				strconv.FormatInt(record.Timestamp.Unix(), 10),
				record.FactoryID,
				record.JobId,
				record.DeviceID,
				strconv.FormatFloat(float64(record.Temperature), 'f', 4, 32),
				strconv.FormatFloat(float64(record.Humidity), 'f', 4, 32),
				strconv.FormatFloat(float64(record.Pressure), 'f', 4, 32),
				strconv.FormatFloat(float64(record.Voltage), 'f', 4, 32),
				strconv.FormatFloat(float64(record.Current), 'f', 4, 32),
				strconv.FormatFloat(float64(record.Power), 'f', 4, 32),
				strconv.FormatInt(record.RPM, 10),
				record.Status,
				strconv.FormatInt(int64(record.ErrorCode), 10),
				strconv.FormatInt(record.ProductionCount, 10),
			}
			batchCSV = append(batchCSV, csvRecord)
		}

		// 批量写入文件
		if err := writer.WriteAll(batchCSV); err != nil {
			return fmt.Errorf("写入批次数据失败: %v", err)
		}

		recordsWritten += len(batch)
		if recordsWritten%100000 == 0 {
			g.logf("已写入 %d/%d 条记录...\n", recordsWritten, g.totalRecords)
		}

		// 清理批次数据，释放内存
		batchCSV = batchCSV[:0]
		writer.Flush()
	}

	g.logf("数据生成完成，共写入 %d 条记录\n", recordsWritten)
	return nil
}

// GetTotalRecords 获取总记录数（用于外部进度跟踪）
func (g *DataGenerator) GetTotalRecords() (int, error) {
	if err := g.initializeMetadata(); err != nil {
		return 0, err
	}
	return g.totalRecords, nil
}

func (g *DataGenerator) logf(format string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	fmt.Printf("[%s] %s", timestamp, fmt.Sprintf(format, args...))
}

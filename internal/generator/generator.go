package generator

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"test-benchmark/internal/config"
	"test-benchmark/internal/models"
)

type DataGenerator struct {
	config *config.DataGenConfig
	rand   *rand.Rand
}

func NewDataGenerator(cfg *config.DataGenConfig) *DataGenerator {
	return &DataGenerator{
		config: cfg,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// JobSchedule 表示Job的调度信息
type JobSchedule struct {
	JobId     int
	StartTime time.Time
	EndTime   time.Time
}

func (g *DataGenerator) GenerateData() ([]models.SensorData, error) {
	// 1. 解析日期并设置起始时间
	dayStart, err := ParseDay(g.config.Day)
	if err != nil {
		return nil, fmt.Errorf("解析日期失败: %v", err)
	}

	// 2. 创建所有的数据采集事件
	events := g.createDataCollectionEvents(dayStart)

	// 3. 事件已经按时间排序，直接按顺序生成数据
	data := make([]models.SensorData, 0, len(events))
	jobAssignments := g.assignJobsToDevices()

	for _, event := range events {
		assignment := jobAssignments[event.JobId-1]
		factoryName := fmt.Sprintf("factory_%03d", assignment.FactoryId)
		deviceName := fmt.Sprintf("device_%03d", assignment.DeviceId)
		jobName := fmt.Sprintf("job_%06d", event.JobId)

		record := g.generateSensorRecord(factoryName, jobName, deviceName, event.Time)
		data = append(data, record)
	}

	return data, nil
}

// DataCollectionEvent 表示一个数据采集事件
type DataCollectionEvent struct {
	Time  time.Time
	JobId int
}

// createDataCollectionEvents 创建所有数据采集事件并按时间排序
func (g *DataGenerator) createDataCollectionEvents(dayStart time.Time) []DataCollectionEvent {
	var events []DataCollectionEvent

	// 计算Job启动时间
	jobStartTimes := g.calculateJobStartTimes(dayStart)
	recordsPerJob := g.calculateRecordsPerJob()
	timeInterval := time.Duration(g.config.TimeInterval) * time.Second

	// 为每个Job创建数据采集事件
	for jobId := 1; jobId <= g.config.JobCount; jobId++ {
		jobStartTime := jobStartTimes[jobId-1]

		// 为该job的每个采集时间点创建事件
		for i := 0; i < recordsPerJob; i++ {
			eventTime := jobStartTime.Add(time.Duration(i) * timeInterval)
			events = append(events, DataCollectionEvent{
				Time:  eventTime,
				JobId: jobId,
			})
		}
	}

	// 按时间排序事件
	sort.Slice(events, func(i, j int) bool {
		return events[i].Time.Before(events[j].Time)
	})

	return events
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

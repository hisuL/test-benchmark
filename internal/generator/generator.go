package generator

import (
	"fmt"
	"math/rand"
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

func (g *DataGenerator) GenerateData() ([]models.SensorData, error) {
	var data []models.SensorData
	now := time.Date(2025, 6, 24, 15, 23, 0, 0, time.UTC)
	startTime := now.Add(-time.Duration(g.config.TimeSpanHours) * time.Hour)
	endTime := now

	// 计算每台设备的采样数
	totalDuration := endTime.Sub(startTime)
	samplingInterval := time.Duration(g.config.SamplingRateMs) * time.Millisecond
	samplesPerDevice := int64(totalDuration / samplingInterval)

	if samplesPerDevice == 0 {
		return nil, fmt.Errorf("sampling interval is too large for the given timespan")
	}

	// 确保设备数量和工厂分布合理
	if g.config.FactoryCount == 0 || g.config.DeviceCount == 0 {
		return nil, fmt.Errorf("invalid configuration: FactoryCount and DeviceCount must be greater than 0")
	}

	// 预分配切片大小
	data = make([]models.SensorData, 0, g.config.TotalRecords)

	// 预生成 Job ID 池
	jobIdPool := make([]string, g.config.JobCount)
	for i := 0; i < g.config.JobCount; i++ {
		jobIdPool[i] = fmt.Sprintf("job_%06d", i+1)
	}

	recordCount := int64(0)

	// 数据生成
	for factoryId := 1; factoryId <= g.config.FactoryCount; factoryId++ {
		factoryName := fmt.Sprintf("factory_%03d", factoryId)

		for recordCount < g.config.TotalRecords {
			// 随机生成 deviceId，范围是 1 到 g.config.DeviceCount（对应当前工厂）
			randomDeviceId := g.rand.Intn(g.config.DeviceCount) + 1
			deviceName := fmt.Sprintf("device_%03d", randomDeviceId) // 随机设备名

			// 遍历时间
			for sample := int64(0); sample < samplesPerDevice && recordCount < g.config.TotalRecords; sample++ {
				currentTime := startTime.Add(time.Duration(sample) * samplingInterval)
				jobId := jobIdPool[g.rand.Intn(len(jobIdPool))] // 从 Job ID 池随机选取
				record := g.generateSensorRecord(factoryName, jobId, deviceName, currentTime)

				data = append(data, record)
				recordCount++
			}
			// 防止超出总记录数
			if recordCount >= g.config.TotalRecords {
				break
			}
		}
	}

	return data, nil
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

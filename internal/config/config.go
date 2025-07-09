package config

import (
	"gopkg.in/yaml.v3"
	"os"
)

type Config struct {
	DataGeneration DataGenConfig   `yaml:"data_generation"`
	Databases      DatabasesConfig `yaml:"databases"`
	Benchmark      BenchmarkConfig `yaml:"benchmark"`
}

type DataGenConfig struct {
	DeviceCount  int    `yaml:"device_count"`  // 设备数量
	FactoryCount int    `yaml:"factory_count"` // 工厂数量
	OutputFile   string `yaml:"output_file"`   // 生成数据的输出文件路径
	JobCount     int    `yaml:"job_count"`     // 生成数据的作业数量
	Day          string `yaml:"day"`           // 生成数据的日期，格式为YYYY-MM-DD
	TimeInterval int    `yaml:"time_interval"` // 采集时间间隔，单位为秒
	JobRunTime   int    `yaml:"job_run_time"`  // 每个job固定运行时间，单位为秒
	BatchSize    int    `yaml:"batch_size"`    // 批量插入的大小
}

type DatabasesConfig struct {
	InfluxDB InfluxDBConfig `yaml:"influxdb"`
	TDengine TDengineConfig `yaml:"tdengine"`
	IoTDB    IoTDBConfig    `yaml:"iotdb"`
}

type InfluxDBConfig struct {
	URL    string `yaml:"url"`
	Token  string `yaml:"token"`
	Org    string `yaml:"org"`
	Bucket string `yaml:"bucket"`
}

type TDengineConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

type IoTDBConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type BenchmarkConfig struct {
	ConcurrencyLevels     []int `yaml:"concurrency_levels"`
	QueryDurationSeconds  int   `yaml:"query_duration_seconds"`
	WarmupDurationSeconds int   `yaml:"warmup_duration_seconds"`
	ReportInterval        int   `yaml:"report_interval"`
}

func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

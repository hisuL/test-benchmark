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
	TotalRecords   int64  `yaml:"total_records"`
	TimeSpanHours  int    `yaml:"time_span_hours"`
	DeviceCount    int    `yaml:"device_count"`
	FactoryCount   int    `yaml:"factory_count"`
	SamplingRateMs int    `yaml:"sampling_rate_ms"`
	BatchSize      int    `yaml:"batch_size"`
	OutputFile     string `yaml:"output_file"`
	JobCount       int    `yaml:"job_count"`
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
	DSN      string `yaml:"dsn"`
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

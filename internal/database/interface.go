package database

import (
	"context"
	"test-benchmark/internal/models"
	"time"
)

type Database interface {
	Name() string
	Connect(ctx context.Context) error
	Close() error

	// Schema operations
	CreateSchema(ctx context.Context) error
	DropSchema(ctx context.Context) error

	// Write operations
	WriteBatch(ctx context.Context, data []models.SensorData) error

	// Query operations
	QueryByDeviceAndTimeRange(ctx context.Context, deviceID string, start, end time.Time) ([]models.SensorData, error)
	QueryAggregation(ctx context.Context, deviceID string, start, end time.Time, aggType string) (float32, error)
	QueryTimeRange(ctx context.Context, start, end time.Time, limit int) ([]models.SensorData, error)
	QueryGroupBy(ctx context.Context, start, end time.Time, groupBy string, interval time.Duration) (map[string]float32, error)

	// Health check
	Ping(ctx context.Context) error

	//GetRandomFactoryId 从数据库里获取一个存在的随机的  factoryId
	GetRandomFactoryId(ctx context.Context) string

	// GetRandomDeviceId 从数据库里获取一个存在的随机的 deviceId
	GetRandomDeviceId(ctx context.Context, deviceId string) string

	// GetStartTime 从数据库里获取指定 factoryId 和 deviceId 的数据的起始时间
	GetStartTime(ctx context.Context, factoryId string, deviceId string) time.Time

	// GetEndTime 从数据库里获取指定 factoryId 和 deviceId 的数据的结束时间
	GetEndTime(ctx context.Context, factoryId string, deviceId string) time.Time

	// RemoveALLData 清除数据库中的所有数据
	RemoveALLData(ctx context.Context) error
}

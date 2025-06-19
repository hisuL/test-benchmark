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
}

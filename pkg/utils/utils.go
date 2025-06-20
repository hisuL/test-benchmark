package utils

import (
	"fmt"
	"time"
)

// FormatDuration formats duration in human-readable format
func FormatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%.0fns", float64(d.Nanoseconds()))
	} else if d < time.Millisecond {
		return fmt.Sprintf("%.2fμs", float64(d.Nanoseconds())/1000)
	} else if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	} else {
		return d.String()
	}
}

// FormatBytes formats bytes in human-readable format
func FormatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "kMGTPE"[exp])
}

// CalculatePercentile calculates percentile from sorted slice
func CalculatePercentile(data []float64, percentile float64) float64 {
	if len(data) == 0 {
		return 0
	}

	if percentile <= 0 {
		return data[0]
	}
	if percentile >= 100 {
		return data[len(data)-1]
	}

	index := (percentile / 100) * float64(len(data)-1)
	lower := int(index)
	upper := lower + 1

	if upper >= len(data) {
		return data[lower]
	}

	weight := index - float64(lower)
	return data[lower]*(1-weight) + data[upper]*weight
}

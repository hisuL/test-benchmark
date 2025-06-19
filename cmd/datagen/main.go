package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"test-benchmark/internal/config"
	"test-benchmark/internal/generator"
)

func main() {
	var (
		configPath   = flag.String("config", "configs/config.yaml", "Path to configuration file")
		outputPath   = flag.String("output", "", "Output file path (overrides config)")
		totalRecords = flag.Int64("records", 0, "Total records to generate (overrides config)")
		deviceCount  = flag.Int("devices", 0, "Number of devices (overrides config)")
		timeSpan     = flag.Int("timespan", 0, "Time span in hours (overrides config)")
	)
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Override config with command line parameters
	if *totalRecords > 0 {
		cfg.DataGeneration.TotalRecords = *totalRecords
	}
	if *deviceCount > 0 {
		cfg.DataGeneration.DeviceCount = *deviceCount
	}
	if *timeSpan > 0 {
		cfg.DataGeneration.TimeSpanHours = *timeSpan
	}
	if *outputPath != "" {
		cfg.DataGeneration.OutputFile = *outputPath
	}

	fmt.Printf("Generating %d records across %d devices over %d hours\n",
		cfg.DataGeneration.TotalRecords,
		cfg.DataGeneration.DeviceCount,
		cfg.DataGeneration.TimeSpanHours)

	// Generate data
	gen := generator.NewDataGenerator(&cfg.DataGeneration)
	data, err := gen.GenerateData()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating data: %v\n", err)
		os.Exit(1)
	}

	// Save to file
	file, err := os.Create(cfg.DataGeneration.OutputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	err = encoder.Encode(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing data: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully generated %d records and saved to %s\n",
		len(data), cfg.DataGeneration.OutputFile)
}

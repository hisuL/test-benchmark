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
		configPath = flag.String("config", "configs/config.yaml", "Path to configuration file")
	)
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

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

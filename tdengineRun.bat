@echo off
echo Running benchmark...

./bin/benchmark -config configs/config.yaml -output results -database tdengine

echo Benchmark completed! Check the results directory for detailed reports.
pause

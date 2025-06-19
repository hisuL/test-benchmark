@echo off
echo Running benchmark...

bin\benchmark.exe -config configs\config.yaml -output results -database influxdb

echo Benchmark completed! Check the results directory for detailed reports.
pause

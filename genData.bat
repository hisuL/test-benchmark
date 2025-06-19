@echo off
 

echo Generating test data...
bin\datagen.exe -config configs/config.yaml
echo Test data generation completed!
pause

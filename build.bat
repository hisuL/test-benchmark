@echo off

del bin\*.exe

echo Building Go application...

set GOOS=windows
set GOARCH=amd64
set CGO_ENABLED=0

go mod tidy
go build -o bin/datagen.exe cmd/datagen/main.go
go build -o bin/benchmark.exe cmd/benchmark/main.go


pause

GOARCH=amd64 go build ../mr


GOARCH=amd64 go build ./mrworker.go
GOARCH=amd64 go build ./mrmaster.go

cd ../mrapps
GOARCH=amd64 go build -buildmode=plugin ../mrapps/wc.go



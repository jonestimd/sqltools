export GO111MODULE=on

# test:
# 	go test --coverprofile cover.out ./cmd/... ./internal/...
# 	go tool cover -html=cover.out -o coverage/go-coverage.html

go_sources := $(shell find internal -name "*.go" ! -name "*_test.go") cmd/cmp-sqldump/cmp-sqldump.go

cmp-sqldump: $(go_sources)
	go build ./cmd/cmp-sqldump

clean:
	rm -f cmp-sqldump cover.out coverage/go-coverage.html

install: cmp-sqldump
	go install ./cmd/cmp-sqldump/

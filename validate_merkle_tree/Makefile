build:
	@echo "Building validation tool..."
	GIT_TERMINAL_PROMPT=1 \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
	go build -o ./bin/validate_merkle_tree ./main.go
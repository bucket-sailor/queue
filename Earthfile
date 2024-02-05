VERSION 0.7
FROM golang:1.21-bookworm
WORKDIR /app

tidy:
  LOCALLY
  RUN go mod tidy
  RUN go fmt ./...

lint:
  FROM golangci/golangci-lint:v1.55.2
  WORKDIR /app
  COPY . ./
  RUN golangci-lint run --timeout 5m ./...

test:
  COPY go.* ./
  RUN go mod download
  COPY . .
  RUN go test -coverprofile=coverage.out -v ./...
  SAVE ARTIFACT ./coverage.out AS LOCAL coverage.out
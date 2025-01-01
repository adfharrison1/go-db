# A minimal Dockerfile
FROM golang:1.20-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /go-db ./cmd/go-db

# Final stage
FROM alpine:3.18
COPY --from=builder /go-db /go-db
EXPOSE 8080
ENTRYPOINT ["/go-db"]

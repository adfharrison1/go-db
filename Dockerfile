# A minimal Dockerfile
FROM golang:1.20-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /go-db ./cmd/go-db.go

# Final stage
FROM alpine:3.18
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /go-db /app/go-db
RUN chmod +x /app/go-db
EXPOSE 8080
ENTRYPOINT ["/app/go-db"]

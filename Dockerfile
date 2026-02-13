# Build stage - Go 1.25
FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY src/go.mod src/go.sum ./
RUN go mod download

COPY src/ .
RUN go build -o main .

# Runtime stage
FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /app/main .

EXPOSE 3000
CMD ["./main", "rest"]
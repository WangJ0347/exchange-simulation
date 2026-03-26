# Build stage
FROM golang:1.23.5-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application from cmd directory
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main ./cmd

# Final stage
FROM alpine:latest

# Install ca-certificates and tzdata for timezone support
RUN apk --no-cache add ca-certificates tzdata

# Set timezone to Ho Chi Minh City (same as Bangkok, UTC+7)
ENV TZ=Asia/Ho_Chi_Minh
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /app/main .

# Expose port 8003 for HTTP API
EXPOSE 8003

# Run the application
CMD ["./main"]

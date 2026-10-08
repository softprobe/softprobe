FROM golang:1.23-alpine AS builder

ARG BUILD_DATE
ENV BUILD_DATE=${BUILD_DATE}

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o main main.go

FROM alpine:latest

# install ca-certificates（required for HTTPS requests）
RUN apk --no-cache add ca-certificates curl jq

WORKDIR /root/

# copy the built binary from the builder stage
COPY --from=builder /app/main .

# expose port
EXPOSE 80

# run the application
CMD ["./main"]

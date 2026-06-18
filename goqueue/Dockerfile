FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY goqueue/go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /goqueue-example ./example/

FROM alpine:3.20
COPY --from=builder /goqueue-example /usr/local/bin/
CMD ["goqueue-example"]

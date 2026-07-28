FROM golang:alpine AS builder
RUN apk add --no-cache git
WORKDIR /src
COPY ProjectionService/go.mod ProjectionService/go.sum ./
RUN go mod download
COPY ProjectionService/ ./
RUN CGO_ENABLED=0 go build -o /go/bin/app -v .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /go/bin/app /app
ENTRYPOINT ["/app"]
LABEL Name=eventsourcingkafkademo Version=0.0.1
EXPOSE 3000

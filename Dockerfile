FROM public.ecr.aws/zomato/golang:1.13 AS build-env
WORKDIR /go/src/kafka-offset-lag-statsd
COPY . .
RUN useradd -u 10001 kafkaoffsetlag

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o kafka-offset-lag-statsd
FROM  alpine
COPY --from=build-env /go/src/kafka-offset-lag-statsd/kafka-offset-lag-statsd .
COPY --from=build-env /etc/passwd /etc/passwd
USER kafkaoffsetlag
ENTRYPOINT ["/kafka-offset-lag-statsd"]

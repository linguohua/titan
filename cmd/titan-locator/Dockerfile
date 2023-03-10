FROM golang:1.19

WORKDIR /locator

COPY ../../go.mod ../../go.sum ./
RUN go mod download

COPY ../../ .

# when compiling with dynamic link function，don't rely on GLIBC
ENV CGO_ENABLED 0

RUN go build -o titan-locator ./cmd/titan-locator && \
    cd /usr && git clone https://github.com/timtide/map.git

FROM alpine:3.17.0

COPY --from=0 /locator/titan-locator /usr/local/titan-locator
COPY --from=0 /usr/map/city.mmdb /usr/local/city.mmdb

# host address and port the locator api will listen on
EXPOSE 5000

ENTRYPOINT ["/usr/local/titan-locator", "run"]

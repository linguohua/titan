FROM golang:1.19

WORKDIR /locator

COPY ../../go.mod ../../go.sum ./
RUN go mod download

COPY ../../ .

# when compiling with dynamic link function，don't rely on GLIBC
ENV CGO_ENABLED 0
# Install deps
# RUN apt-get update && apt-get install -y \
#  libssl-dev \
#  ca-certificates \
#  fuse
RUN go build -o titan-locator ./cmd/titan-locator && \
    cd /usr && git clone https://github.com/timtide/map.git

FROM alpine:3.17.0

COPY --from=0 /locator/titan-locator /usr/local/titan-locator
COPY --from=0 /usr/map/city.mmdb /usr/local/city.mmdb

# host address and port the worker api will listen on
EXPOSE 5000

ENTRYPOINT ["/usr/local/titan-locator", "run", "--geodb-path=/usr/local/city.mmdb"]
CMD ["--accesspoint-db='root:root@tcp(127.0.0.1:3306)/locator'"]

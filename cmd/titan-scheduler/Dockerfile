FROM golang:1.19

WORKDIR /scheduler

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
RUN go build -o titan-scheduler ./cmd/titan-scheduler && \
    cd /usr && git clone https://github.com/timtide/map.git

FROM alpine:3.17.0

COPY --from=0 /scheduler/titan-scheduler /usr/local/titan-scheduler
COPY --from=0 /usr/map/city.mmdb /usr/local/city.mmdb

# host address and port the scheduler api will listen on
EXPOSE 3456

ENTRYPOINT ["/usr/local/titan-scheduler", "run", "--geodb-path=/usr/local/city.mmdb"]
CMD ["--server-name=adam"]

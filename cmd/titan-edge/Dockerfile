FROM golang:1.19

WORKDIR /edge

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
RUN go build -o titan-edge ./cmd/titan-edge

FROM alpine:3.17.0

ENV EDGE_PATH=~/.titanedge
ENV LOCATOR_API_INFO=http://192.168.0.132:5000

RUN mkdir -p ~/.titanedge
COPY --from=0 /edge/titan-edge /usr/local/titan-edge

# download server address for who download block, example: --download-srv-addr=127.0.0.1:3000
EXPOSE 3000
# host address and port the edge api will listen on
EXPOSE 1234

ENTRYPOINT ["/usr/local/titan-edge", "run", "--locator=true"]
CMD ["--locator=true"]

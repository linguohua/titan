FROM golang:1.19

ENV SRC_DIR /edge

# Install deps
RUN apt-get update && apt-get install -y \
  libssl-dev \
  ca-certificates \
  fuse

WORKDIR $SRC_DIR

COPY . .

ENV CGO_ENABLED 0
RUN go mod download && go build -o titan-edge ./cmd/titan-edge


FROM alpine:3.17.0

RUN mkdir -p ~/.titanedge
COPY --from=0 /edge/titan-edge /usr/local/titan-edge

EXPOSE 3000
EXPOSE 3456
ENV EDGE_PATH=~/.titanedge
ENV LOCATOR_API_INFO=http://192.168.0.132:5000

ENTRYPOINT ["/usr/local/titan-edge", "run"]
CMD ["--locator=true"]

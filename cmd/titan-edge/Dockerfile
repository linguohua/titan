FROM golang:1.19

WORKDIR /edge

COPY ../../go.mod ../../go.sum ./

RUN go mod download

COPY ../../ .

# when compiling with dynamic link function，don't rely on GLIBC
ENV CGO_ENABLED 0

RUN go build -o titan-edge ./cmd/titan-edge

FROM alpine:3.17.0

COPY --from=0 /edge/titan-edge /usr/local/titan-edge

# host address and port the edge api will listen on
EXPOSE 1234

ENTRYPOINT ["/usr/local/titan-edge", "run",]

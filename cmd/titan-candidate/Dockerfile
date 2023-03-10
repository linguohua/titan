FROM golang:1.19

WORKDIR /candidate

COPY ../../go.mod ../../go.sum ./
RUN go mod download

COPY ../../ .

# when compiling with dynamic link function，don't rely on GLIBC
ENV CGO_ENABLED 0

RUN go build -o titan-candidate ./cmd/titan-candidate

FROM alpine:3.17.0

COPY --from=0 /candidate/titan-candidate /usr/local/titan-candidate

# download server address for who download block, example: --download-srv-addr=127.0.0.1:3000
EXPOSE 9000
# host address and port the candidate api will listen on
EXPOSE 2345

ENTRYPOINT ["/usr/local/titan-candidate", "run"]

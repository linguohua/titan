FROM golang:1.19

WORKDIR /candidate

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
RUN go build -o titan-candidate ./cmd/titan-candidate

FROM alpine:3.17.0

ENV CANDIDATE_PATH=~/.titancandidate
ENV LOCATOR_API_INFO=http://192.168.0.132:5000
ENV IPFS_API=http://192.168.0.132:5001

RUN mkdir -p ~/.titancandidate
COPY --from=0 /candidate/titan-candidate /usr/local/titan-candidate

# download server address for who download block, example: --download-srv-addr=127.0.0.1:3000
EXPOSE 3000
# host address and port the candidate api will listen on
EXPOSE 2345

ENTRYPOINT ["/usr/local/titan-candidate", "run", "--locator=true"]
CMD ["--locator=true"]

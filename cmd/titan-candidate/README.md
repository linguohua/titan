## build

### first method
```shell
cd ../../

docker build -t candidate:latest -f ./cmd/titan-candidate/Dockerfile .
```

### two method
```shell
cd ../../

make candidate-image
```

generate images in the local docker warehouse after construction, image name:candidate, tag:latest.


## run

Before running, you need to apply for the device ID and secret in advance for the parameters '--device-id' and 'secret'

### data mount
the local data mount must be established, and the internal path of the mapping container must be '/root/.titancandidate', eg:
```shell
docker run -d --name candidate -p 3000:3000 -p 2345:2345 -v /Users/jason/.titancandidate:/root/.titancandidate candidate:latest \
--device-id=c_93a7edd5575f40b6abf2d0f988c473ae \
--secret=a120de6e2345607accfe9c03109f0b1974a9990b
```

### default port operation

```shell
docker run -d --name candidate -p 3000:3000 -p 2345:2345 -v /Users/jason/.titancandidate:/root/.titancandidate candidate:latest \
--device-id=c_93a7edd5575f40b6abf2d0f988c473ae \
--secret=a120de6e2345607accfe9c03109f0b1974a9990b
```

### specified port operation
ensure the consistency of internal and external ports
```shell
docker run -d --name candidate -p 7777:7777 -p 6666:6666 -v /Users/jason/.titancandidate:/root/.titancandidate candidate:latest \
--device-id=c_93a7edd5575f40b6abf2d0f988c473ae \
--secret=a120de6e2345607accfe9c03109f0b1974a9990b \
--listen=0.0.0.0:6666 \
--download-srv-addr=0.0.0.0:7777
```

## build

### first method
```shell
cd ../../

docker build -t edge:latest -f ./cmd/titan-edge/Dockerfile .
```

### two method
```shell
cd ../../

make edge-image
```

generate images in the local docker warehouse after construction,image name:edge,tag:latest.


## run

Before running, you need to apply for the device ID and secret in advance for the parameters '--device-id' and 'secret'

### data mount
the local data mount must be established, and the internal path of the mapping container must be '/root/.titanedge', eg:
```shell
docker run -d --name edge -p 1234:1234 \
 -v /Users/jason/.titanedge:/root/.titanedge \
 -e LOCATOR_API_INFO=https://39.108.143.56:5000 edge:latest \
 --device-id=e_65fc28b2d7d640d59147902df31a4735 \
 --secret=5a3649de4f23bda613171be2fa5b3acdc24bd1f4
```


### specified port operation
ensure the consistency of internal and external ports
```shell
docker run -d --name edge -p 6666:6666 \
 -v /Users/jason/.titanedge:/root/.titanedge edge:latest \
 -e --TITAN_LISTENER=0.0.0.0:6666
 --device-id=e_65fc28b2d7d640d59147902df31a4735 \
 --secret=5a3649de4f23bda613171be2fa5b3acdc24bd1f4 
```

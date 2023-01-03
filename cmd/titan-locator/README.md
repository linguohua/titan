## build

### first method
```shell
cd ../../

docker build -t locator:latest -f ./cmd/titan-locator/Dockerfile .
```

### two method
```shell
cd ../../

make locator-image
```

generate images in the local docker warehouse after construction, image name:locator, tag:latest.


## run

### default port operation
```shell
docker run -d --name locator -p 5000:5000 locator:latest --accesspoint-db='root:123456@tcp(127.0.0.1:3306)/locator'
```

### specified port operation
ensure the consistency of internal and external ports
```shell
docker run -d --name locator -p 5555:5555 locator:latest --accesspoint-db='root:123456@tcp(127.0.0.1:3306)/locator' \
--listen=0.0.0.0:5555
```

notice: the database address must be a specific address,
eg: If your local IP address is 192.168.0.1, you must replace the above 127.0.0.1

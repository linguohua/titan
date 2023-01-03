## build

### first method
```shell
cd ../../

docker build -t scheduler:latest -f ./cmd/titan-scheduler/Dockerfile .
```

### two method
```shell
cd ../../

make scheduler-image
```

generate images in the local docker warehouse after construction, image name:scheduler, tag:latest.


## run

### default port operation
```shell
docker run -d --name scheduler -p 3456:3456 scheduler:latest \
--server-name=test \
--persistentdb-url='root:123456@tcp(127.0.0.1:3306)/titan' \
--cachedb-url 127.0.0.1:6379
```

### specified port operation
ensure the consistency of internal and external ports
```shell
docker run -d --name scheduler -p 6789:6789 scheduler:latest \
--server-name=test \
--persistentdb-url='root:123456@tcp(127.0.0.1:3306)/titan' \
--cachedb-url 127.0.0.1:6379 \
--listen=0.0.0.0:6789
```

notice: the database address must be a specific address, 
eg: If your local IP address is 192.168.0.1, you must replace the above 127.0.0.1

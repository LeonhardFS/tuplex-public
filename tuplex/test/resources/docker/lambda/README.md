### 1. Build docker image
`docker build -t tuplex-local-lambda .`

### 2. Run local image
```
docker run -p 9000:8080 tuplex-local-lambda
```

### 3. Request towards Lambda
```
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"payload":"hello world!"}'
```
# 用于测试环境docker部署, 非k8s部署
docker stop sqlsyncify-api
docker rm sqlsyncify-api
docker rmi sqlsyncify:1.0

docker build -t sqlsyncify:1.0 -f docker/Dockerfile .

docker run -d \
  -e APP_ENV=production \
  -v ./etc:/app/etc \
  -v ./storage:/app/storage \
  -p 8080:8080 \
  --name sqlsyncify-api \
  sqlsyncify:1.0

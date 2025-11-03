# https://www.cnblogs.com/kevinwan/p/16033634.html
FROM golang:alpine AS builder
# 安装 gcc、musl-dev、和其他构建工具, cgo需要gcc
# --no-cache
RUN apk add  gcc musl-dev

WORKDIR /app

COPY . .
ENV CGO_ENABLED 1
# ENV GOPROXY https://goproxy.cn,direct
ENV GO111MODULE on
RUN go mod download
# -ldflags="-s -w" 表示去掉调试信息,以减小编译出来的文件尺寸
# -w:去掉 dwarf 调试信息。会减小可执行文件的大小
# -s:去掉符号表信息。会进一步减小可执行文件的大小
# 在去掉调试信息和符号表信息后，如果出现了异常（如 panic），会导致排查变得困难
RUN go build -ldflags="-s -w" -o ./bin/sqlsyncify .

# 从上边的镜像中抽取出编译后的文件
FROM alpine:latest
# alpine安装 tzdata 时区数据包
# --no-cache
RUN apk add tzdata

WORKDIR /app
COPY --from=builder /app/bin/sqlsyncify /app/sqlsyncify

# 设置时区为 Asia/Shanghai
ENV TZ=Asia/Shanghai
# 应用时区设置
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 设置固定的项目路径
RUN chmod +x /app/sqlsyncify

ENV APP_ENV production

# 添加配置文件
ADD etc /app/etc
ADD storage /app/storage

EXPOSE 8080

CMD ["./sqlsyncify", "-f", "etc/sqlsyncify-api.yaml"]

# 说明

此工程用于导出网站(mysql)数据, 本地组装数据后导入es
基于go-zero框架中运行
SQLite 自 3.25.0 版本开始支持窗口函数，就能实现hive的函数实现

```
-- 连接sqlitte .db文件后执行sql可以看版本
SELECT sqlite_version()
```

同时支持: es5.6, es8
网站：local56 = es5.6
网站：local8 = es8


## 使用方式

1. 全量更新
   1. http://localhost:8080/sync/all/{site}
2. 全量更新-不导入只导出
   1. http://localhost:8080/sync/all/{site}?import=0
3. 清理没有别名的索引
   1. curl -vv http://localhost:8080/clean/noalias
   1. curl -vv http://localhost:8080/clean/noalias/v5
4. 获取同义词配置,在es-index:mapping中定义使用
   1. HEAD http://localhost:8080/synonym/{site}/{lang}
   2. GET http://localhost:8080/synonym/{site}/{lang}
   3. curl -vv --head -H "If-Modified-Since:" -H "If-None-Match:" "http://localhost:8080/synonym/wordpress/en"
   4. curl -vv http://localhost:8080/synonym/wordpress/en

## 数据同步流程

1. 导出SQL抽取远程源数据,写入本地sqlite表，一个站一个.db文件，一个SQL一张表
2. 导出SQL读取本地sqlite, 合并多表组装成ES需要的格式
3. 调用http接口发起推送es,可选择手工触发，或者定时任务工具触发

## 导出数据的SQL

在 etc/sql-import,etc/sql-export 目录

# 生成api,要创建新api时才要执行

1. 修改好 sqlsyncify.api
2. 执行
```
goctl api go --api sqlsyncify.api --dir .

```
# pprof性能分析
## 加入
```
import (
   "net/http"
	_ "net/http/pprof"
)

func main() {
   //在Start()前
   go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()
   //...

   server.Start()
   
}

```

## 浏览器查看
http://localhost:6060/debug/pprof

## 查看内存占用
go tool pprof --text http://localhost:6060/debug/pprof/heap

## 打开web分析
go tool pprof -http=:7778 http://localhost:6060/debug/pprof/heap


# Sqlsyncify

Sqlsyncify是一个用于将MySQL数据导出到ElasticSearch的数据同步工具

特点是低成本运行, 多段SQL分批导出MySQL，SQLite本地组装数据实现自定义大文档，多协程并发推送数据
可用于替换Spark写入ES的高成本运行方式

目前只做了全量同步

基于go-zero框架运行

SQLite 自 3.25.0 版本开始支持窗口函数，就能实现hive的CONCAT_WS + collect_set

```
-- 连接sqlitte .db文件后执行sql可以看版本
SELECT sqlite_version()
```

同时支持: es5.6, es8(esapi通用，es6、7未验证)
```
同义词插件依赖
https://github.com/sqlsyncify/elasticsearch-analysis-dynamic-synonym
forked from https://github.com/bells/elasticsearch-analysis-dynamic-synonym
修复过8.7下的编译
es5.6和es8的setting的同义词配置不同，要注意

es数据推送依赖
https://github.com/elastic/go-elasticsearch/v8
https://github.com/elastic/go-elasticsearch/v5
```

## 数据同步流程

1. SQL抽取远程MySQL源数据,写入本地SQLite表，一个站一个.db文件，一个SQL一张表
2. SQL读取本地SQLite, 合并多表组装成ES需要的格式
3. 调用es http接口发起推送es, 可选择手工触发，或者定时任务触发

## 使用方式

1. 全量更新
   1. http://localhost:8080/sync/all/{site}
1. 全量更新-只导入mysql数据到本地SQLite, 不导出到es
    1. http://localhost:8080/sync/all/{site}?export=0
1. 全量更新-不导入mysql, 只导出SQLite到es
    1. http://localhost:8080/sync/all/{site}?import=0
1. 清理没有别名的索引
   1. curl -vv http://localhost:8080/clean/noalias
   1. curl -vv http://localhost:8080/clean/noalias/v5
1. 获取同义词配置, 在mapping中定义使用
   1. HEAD http://localhost:8080/synonym/{site}/{lang}
   2. GET http://localhost:8080/synonym/{site}/{lang}
   3. curl -vv --head -H "If-Modified-Since:" -H "If-None-Match:" "http://localhost:8080/synonym/wordpress/en"
   4. curl -vv http://localhost:8080/synonym/wordpress/en

## 数据源定义

在 etc/datasources 目录, 一个数据源做一个文件

## 导出MYSQL数据的SQL

在 etc/sites/{site}sql-import 目录

## 导入ES的SQL

在 etc/sites/{site}/sql-export 目录

# 增加新的api接口方法
- 不是必须，直接修改internal目录，依照添加go代码，需要熟悉go-zero即可

1. 修改好 sqlsyncify.api
2. 执行
```
goctl api go --api sqlsyncify.api --dir .

```

# pprof性能分析
- sqlsyncify默认集成了pprof, 默认禁用
- 开启办法：
  - 设置环境变量 APP_DEBUG=true
  - 重启应用

## 浏览器查看pprof性能分析
http://localhost:6060/debug/pprof

## 查看内存占用
go tool pprof --text http://localhost:6060/debug/pprof/heap

## 查看web分析
go tool pprof -http=:7778 http://localhost:6060/debug/pprof/heap

浏览器打开 http://localhost:7778


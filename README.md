# Sqlsyncify

Sqlsyncify 是一个高效的 MySQL 到 ElasticSearch 数据同步工具。

## 主要特点

- 低成本运行：无需依赖昂贵的大数据处理框架
- 分批处理：多段 SQL 分批导出 MySQL 数据
- 本地数据组装：使用 SQLite 在本地组装自定义大文档
- 高性能推送：多协程并发推送数据到 ElasticSearch
- 灵活配置：支持自定义数据源和映射规则
- 版本兼容：支持 ElasticSearch 5.6 和 8.x 版本

## 适用场景

- 全量数据同步：定期运行，补全可能缺失的数据
- 混合同步模式：可与实时同步系统配合使用
- 大文档数据同步：适合需要多表关联的复杂数据结构

## 技术依赖

### 基础框架
- 基于 go-zero 框架开发
- 使用 SQLite 3.25.0+ (支持窗口函数)

### ElasticSearch 插件支持
- 同义词插件：[elasticsearch-analysis-dynamic-synonym](https://github.com/sqlsyncify/elasticsearch-analysis-dynamic-synonym)
  - 已针对 ES 8.7 版本优化
  - 支持 ES 5.6 和 8.x 不同配置模式
- ES 客户端：
  - ES 8.x: github.com/elastic/go-elasticsearch/v8
  - ES 5.x: github.com/elastic/go-elasticsearch/v5

## 工作流程

1. **数据抽取**
   - 从 MySQL 源数据库抽取数据
   - 写入本地 SQLite 数据库（每个站点独立的 .db 文件）
   - 每个 SQL 查询结果存储为独立表

2. **数据组装**
   - 读取本地 SQLite 数据
   - 通过 SQL 合并多表数据
   - 按 ES 文档格式组装数据

3. **数据推送**
   - 调用 ES HTTP 接口推送数据
   - 支持手动触发或定时任务

## API 接口

### 同步接口
```
# 全量更新
GET http://localhost:8080/sync/all/{site}

# 仅导入 MySQL 数据到 SQLite
GET http://localhost:8080/sync/all/{site}?export=0

# 仅从 SQLite 导出到 ES
GET http://localhost:8080/sync/all/{site}?import=0
```

### 索引管理接口
```
# 清理无别名索引
GET http://localhost:8080/clean/noalias
GET http://localhost:8080/clean/noalias/v5
```

### 同义词配置接口
```
# 获取同义词配置
HEAD/GET http://localhost:8080/synonym/{site}/{lang}
```

## 项目配置

### 配置文件结构
- `etc/datasources/`: 数据源配置目录
- `etc/sites/{site}/sql-import/`: MySQL 导出 SQL 配置
- `etc/sites/{site}/sql-export/`: ES 导入 SQL 配置

## 开发指南

### 添加新 API 接口
1. 修改 `sqlsyncify.api` 文件
2. 执行命令生成代码：
```bash
goctl api go --api sqlsyncify.api --dir .
```

## 性能分析

### 启用 pprof
- 设置环境变量：`APP_DEBUG=true`
- 重启应用

### 性能监控工具
```bash
# 查看性能分析面板
http://localhost:6060/debug/pprof

# 查看内存占用
go tool pprof --text http://localhost:6060/debug/pprof/heap

# 图形化分析界面
go tool pprof -http=:7778 http://localhost:6060/debug/pprof/heap
```

## 许可证

MIT

# 飞书多维表格 ORM


## 概念
飞书表格概述参考：https://open.feishu.cn/document/server-docs/docs/bitable-v1/bitable-overview

主要概念如下：

- 多维表格：一个类型为多维表格的文件，可以包含多个数据表，类比 Database。
- 数据表：存储数据的单元表格，默认包含隐藏索引字段 `record_id`，类比 Table。
- 视图：视图会限定看到的数据，但不会改变数据，操作视图实际上还是在操作对应的数据表，类比筛选后的 Table。


## Roadmap

- 多维表格
  - [x] 获取多维表格元数据
- 数据表（暂不考虑）
- 视图（暂不考虑）
- 记录
  - [x] 新增记录
  - [x] 更新记录
  - [x] 查询记录
  - [x] 删除记录
  - [x] 新增多条记录
  - [ ] 更新多条记录
  - [x] 批量获取记录
  - [ ] 删除多条记录

## 使用示例

### 批量获取记录（通过记录ID）

```go
// 1. 通过记录ID数组批量获取记录
recordIds := []string{"recordId1", "recordId2", "recordId3"}
records, _ := client.Base("your_app_token").Table("your_table_id").BatchGet(recordIds)
```

**注意事项：**
- 一次最多传入100个记录ID，超出部分将会被忽略
- 这个方法比使用Where条件查询更高效，专门用于通过记录ID批量查询场景
- 使用BatchGetRecords时，Where条件会被忽略
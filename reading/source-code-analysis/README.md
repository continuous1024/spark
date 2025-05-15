# Spark 核心组件源码详解

本目录包含对 Apache Spark 核心组件源码的详细分析，解释关键算法、性能优化技巧和内部机制。通过理解 Spark 的源码实现，我们可以更好地利用 Spark 进行大数据处理，并在必要时对其进行扩展和优化。

## 目录

### 1. [RDD 实现原理](01-rdd-implementation.md)

- RDD 抽象类
- RDD 依赖关系
- RDD 转换操作实现
- RDD 行动操作实现

### 2. [任务调度器](02-task-scheduler.md)

- DAGScheduler 实现
- TaskScheduler 实现
- SchedulerBackend 实现

### 3. [Shuffle 机制](03-shuffle.md)

- Shuffle 概述
- ShuffleManager 接口
- SortShuffleManager 实现
- ShuffleWriter 实现
- ShuffleReader 实现
- ExternalSorter 实现
- IndexShuffleBlockResolver 实现
- ShuffleBlockFetcherIterator 实现
- Shuffle 性能优化

### 4. [存储管理](04-storage-management.md)

- BlockManager 实现
- MemoryManager 实现

### 5. [Spark SQL 组件](05-spark-sql.md)

- SparkSession 实现
- DataFrame/Dataset 实现
- 逻辑计划
- 优化规则
- 物理计划
- 代码生成
- 查询执行引擎
- 自适应查询执行 (AQE)

### 6. [Structured Streaming 组件](06-structured-streaming.md)

- StreamExecution 架构
- MicroBatchExecution 实现
- ContinuousExecution 实现
- 状态存储
- 触发器
- 输出接收器
- 事件时间处理
- 查询计划生成
- 性能优化

### 7. [性能优化技巧](07-performance-optimization.md)

- 内存优化
- Shuffle 优化
- 任务调度优化

## 如何使用本文档

本文档适用于以下场景：

- 深入理解 Spark 内部工作原理
- 调试 Spark 应用程序中的复杂问题
- 为 Spark 贡献代码或开发扩展
- 优化 Spark 应用程序性能

每个章节都包含详细的源码分析和示例，帮助读者理解 Spark 的设计和实现。

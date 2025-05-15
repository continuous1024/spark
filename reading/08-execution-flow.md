# Spark 核心组件执行流程

本文档详细介绍了Apache Spark中核心组件的执行流程，包括任务提交、作业调度和执行阶段。

## 1. 任务提交流程

### 1.1 应用程序提交过程

Spark应用程序的提交过程如下：

```mermaid
sequenceDiagram
    participant Client as 客户端
    participant Master as 集群管理器
    participant Worker as Worker节点
    participant Driver as Driver程序
    participant Executor as Executor

    Client->>Master: 提交应用程序
    Master->>Worker: 分配资源启动Driver
    Worker->>Driver: 启动Driver程序
    Driver->>Master: 注册应用程序
    Driver->>Master: 请求资源
    Master->>Worker: 分配资源启动Executor
    Worker->>Executor: 启动Executor
    Executor->>Driver: 注册Executor
    Driver->>Executor: 分发任务
```

### 1.2 不同部署模式

Spark支持三种部署模式：

1. **客户端模式(Client Mode)**：
   - Driver程序在提交应用的客户端运行
   - 适合交互式应用和调试

2. **集群模式(Cluster Mode)**：
   - Driver程序在集群中的Worker节点上运行
   - 适合生产环境

3. **本地模式(Local Mode)**：
   - 所有组件在单机上运行
   - 适合开发和测试

### 1.3 资源分配过程

```mermaid
graph TD
    A[应用程序提交] --> B[Driver启动]
    B --> C[Driver向集群管理器注册]
    C --> D[集群管理器分配资源]
    D --> E[Worker启动Executor]
    E --> F[Executor向Driver注册]
    F --> G[Driver分发任务]
```

资源分配参数：

- `--num-executors`：Executor数量
- `--executor-cores`：每个Executor的CPU核心数
- `--executor-memory`：每个Executor的内存
- `--driver-memory`：Driver内存

## 2. 作业调度过程

### 2.1 从RDD到物理执行计划

当在RDD上调用行动操作时，Spark会创建执行计划：

```mermaid
graph TD
    A[RDD图] --> B[DAGScheduler]
    B --> C[划分Stage]
    C --> D[创建TaskSet]
    D --> E[TaskScheduler]
    E --> F[SchedulerBackend]
    F --> G[Executor]
    G --> H[执行Task]
```

### 2.2 DAGScheduler工作流程

DAGScheduler是Spark的高级调度器，负责将RDD操作转换为Stage。

```mermaid
sequenceDiagram
    participant App as 应用程序
    participant RDD
    participant DAG as DAGScheduler
    participant Task as TaskScheduler

    App->>RDD: 调用行动操作
    RDD->>DAG: 提交作业
    DAG->>DAG: 创建JobWaiter
    DAG->>DAG: 构建RDD依赖图
    DAG->>DAG: 根据宽依赖划分Stage
    DAG->>DAG: 提交缺失的Stage
    DAG->>DAG: 创建TaskSet
    DAG->>Task: 提交TaskSet
    Task->>App: 返回结果
```

DAGScheduler的主要职责：

1. 根据RDD的依赖关系构建DAG
2. 在宽依赖处划分Stage
3. 确定Stage之间的依赖关系
4. 将Stage转换为TaskSet
5. 处理任务失败和重试

### 2.3 TaskScheduler工作流程

TaskScheduler是Spark的低级调度器，负责将TaskSet中的任务分配给Executor。

```mermaid
sequenceDiagram
    participant DAG as DAGScheduler
    participant Task as TaskScheduler
    participant Backend as SchedulerBackend
    participant Executor

    DAG->>Task: 提交TaskSet
    Task->>Task: 创建TaskSetManager
    Backend->>Task: 资源提供(resourceOffers)
    Task->>Task: 任务调度和分配
    Task->>Backend: 启动任务(launchTasks)
    Backend->>Executor: 执行任务
    Executor->>Task: 任务完成/失败
    Task->>DAG: 任务完成/失败
```

TaskScheduler的主要职责：

1. 维护待执行的TaskSet队列
2. 根据调度策略分配任务
3. 监控任务执行状态
4. 处理任务失败和重试
5. 实现任务本地性优化

### 2.4 调度策略

Spark支持多种调度策略：

1. **FIFO调度(FIFO Scheduling)**：
   - 按照作业提交顺序执行
   - 简单但可能导致小作业等待时间长

2. **公平调度(Fair Scheduling)**：
   - 在多个作业之间公平分配资源
   - 支持作业池和权重

配置调度策略：

```
spark.scheduler.mode=FAIR
```

## 3. 执行阶段详解

### 3.1 Stage划分

Spark根据RDD的依赖关系将作业划分为多个Stage：

1. **依赖类型**：
   - **窄依赖(Narrow Dependency)**：父RDD的每个分区最多被一个子RDD的分区使用
   - **宽依赖(Wide Dependency)**：父RDD的分区可能被多个子RDD的分区使用，需要Shuffle

2. **Stage类型**：
   - **ShuffleMapStage**：产生Shuffle数据的Stage
   - **ResultStage**：返回结果的Stage

```mermaid
graph TD
    A[RDD A] --> B[RDD B]
    B --> C[RDD C]
    C -->|宽依赖| D[RDD D]
    D --> E[RDD E]
    E -->|宽依赖| F[RDD F]

    subgraph Stage 1
        A
        B
        C
    end

    subgraph Stage 2
        D
        E
    end

    subgraph Stage 3
        F
    end
```

### 3.2 Task创建和执行

每个Stage包含多个Task，每个Task处理一个RDD分区：

```mermaid
sequenceDiagram
    participant DAG as DAGScheduler
    participant Task as TaskScheduler
    participant Executor

    DAG->>DAG: 创建Stage
    DAG->>DAG: 为每个分区创建Task
    DAG->>Task: 提交TaskSet
    Task->>Executor: 分发Task
    Executor->>Executor: 执行Task
    Executor->>Task: 返回结果
    Task->>DAG: 通知Task完成
    DAG->>DAG: 如果Stage完成，提交下一个Stage
```

Task类型：

1. **ShuffleMapTask**：执行ShuffleMapStage中的任务，产生Shuffle数据
2. **ResultTask**：执行ResultStage中的任务，返回结果给Driver

### 3.3 Shuffle过程

Shuffle是Spark中最复杂和最昂贵的操作，涉及数据的重新分区和网络传输。

```mermaid
graph TD
    subgraph Map阶段
        A[Task 1] --> B[ShuffleMapOutputWriter]
        C[Task 2] --> D[ShuffleMapOutputWriter]
        E[Task 3] --> F[ShuffleMapOutputWriter]
    end

    B --> G[Shuffle文件1]
    D --> H[Shuffle文件2]
    F --> I[Shuffle文件3]

    subgraph Reduce阶段
        J[Task 1] --> K[ShuffleReader]
        L[Task 2] --> M[ShuffleReader]
    end

    G --> K
    H --> K
    I --> K
    G --> M
    H --> M
    I --> M
```

Shuffle过程：

1. **Map阶段**：
   - 每个Mapper任务计算结果并按照分区写入本地磁盘
   - 使用ShuffleWriter将数据写入分区文件

2. **Reduce阶段**：
   - 每个Reducer任务从所有Mapper获取对应分区的数据
   - 使用ShuffleReader读取和合并数据

Shuffle实现：

1. **Hash Shuffle**：简单但可能产生大量小文件
2. **Sort Shuffle**：排序数据并合并文件，减少文件数量
3. **Tungsten Sort Shuffle**：内存优化版本的Sort Shuffle

### 3.4 任务执行详情

Task在Executor上的执行流程：

```mermaid
sequenceDiagram
    participant Driver
    participant Executor
    participant TaskRunner
    participant Task

    Driver->>Executor: 发送序列化的Task
    Executor->>Executor: 反序列化Task
    Executor->>TaskRunner: 创建TaskRunner
    Executor->>TaskRunner: 提交到线程池
    TaskRunner->>Task: 运行Task
    Task->>Task: 执行计算
    Task->>TaskRunner: 完成计算
    TaskRunner->>Executor: 返回结果
    Executor->>Driver: 发送结果
```

Task执行阶段：

1. **任务反序列化**：反序列化Task及其依赖
2. **执行准备**：设置上下文和资源
3. **运行计算**：执行实际计算
4. **结果处理**：序列化结果并返回给Driver

## 4. SQL查询执行流程

### 4.1 SQL解析和优化

Spark SQL查询的执行流程：

```mermaid
graph TD
    A[SQL查询] --> B[解析器]
    B --> C[未解析的逻辑计划]
    C --> D[分析器]
    D --> E[已解析的逻辑计划]
    E --> F[优化器]
    F --> G[优化后的逻辑计划]
    G --> H[物理计划生成器]
    H --> I[物理执行计划]
    I --> J[代码生成]
    J --> K[RDD执行]
```

### 4.2 物理计划生成

SparkPlanner将逻辑计划转换为物理计划：

```mermaid
sequenceDiagram
    participant Query as 查询
    participant Catalyst as Catalyst优化器
    participant Planner as SparkPlanner
    participant Executor as 执行引擎

    Query->>Catalyst: 提交查询
    Catalyst->>Catalyst: 生成逻辑计划
    Catalyst->>Catalyst: 优化逻辑计划
    Catalyst->>Planner: 转换为物理计划
    Planner->>Planner: 应用物理策略
    Planner->>Planner: 生成多个候选计划
    Planner->>Planner: 选择最佳计划
    Planner->>Executor: 执行物理计划
    Executor->>Query: 返回结果
```

物理策略包括：

- DataSourceStrategy：处理数据源
- JoinSelection：选择最佳连接算法
- Aggregation：优化聚合操作
- SpecialLimits：优化LIMIT操作
- InMemoryScans：处理缓存数据

### 4.3 Whole-Stage Code Generation

Spark SQL使用Whole-Stage Code Generation技术优化执行：

```mermaid
graph TD
    A[物理计划] --> B[检查是否支持代码生成]
    B --> C{支持代码生成?}
    C -->|是| D[生成Java代码]
    C -->|否| E[解释执行]
    D --> F[编译为字节码]
    F --> G[执行生成的代码]
    E --> H[执行]
    G --> I[结果]
    H --> I
```

代码生成优势：

1. 减少虚函数调用
2. 减少内存访问
3. CPU缓存友好
4. 允许JIT优化

## 5. 流处理执行流程

### 5.1 Spark Streaming (DStream)

Spark Streaming将流处理转换为一系列小批量处理：

```mermaid
sequenceDiagram
    participant Source as 数据源
    participant Receiver as 接收器
    participant JobScheduler as 作业调度器
    participant DStream
    participant RDD
    participant Output as 输出

    Source->>Receiver: 发送数据
    Receiver->>Receiver: 存储数据块
    loop 每个批次间隔
        JobScheduler->>DStream: 生成RDD
        DStream->>RDD: 创建批次RDD
        JobScheduler->>RDD: 处理批次
        RDD->>Output: 输出结果
    end
```

### 5.2 Structured Streaming

Structured Streaming是基于Spark SQL引擎的流处理模型：

```mermaid
graph TD
    A[输入源] --> B[StreamExecution]
    B --> C[微批处理/连续处理]
    C --> D[增量查询执行]
    D --> E[状态管理]
    E --> F[输出接收器]
    G[触发器] --> B
    H[Checkpoint] --> B
```

执行模式：

1. **微批处理(Micro-Batch Processing)**：
   - 定期处理小批量数据
   - 使用MicroBatchExecution实现
   - 提供端到端一次性语义

2. **连续处理(Continuous Processing)**：
   - 低延迟处理每条记录
   - 使用ContinuousExecution实现
   - 提供至少一次语义

# Spark 数据流向

本文档详细介绍了Apache Spark中的数据流向，包括数据读取、转换处理和结果输出的完整流程。

## 1. RDD数据流

### 1.1 RDD创建流程

RDD可以通过多种方式创建：

1. **从外部存储系统读取**：
   - 本地文件系统
   - HDFS
   - HBase
   - 各种数据库

2. **从内存中的集合创建**：
   - `parallelize()`方法

3. **从其他RDD转换而来**：
   - 通过转换操作

```mermaid
graph TD
    A[外部存储] -->|textFile/hadoopFile| B[RDD]
    C[内存集合] -->|parallelize| B
    B -->|map/filter等转换| D[新RDD]
```

### 1.2 RDD转换执行流程

RDD操作分为两类：

1. **转换操作(Transformations)**：创建新的RDD，但不执行计算
2. **行动操作(Actions)**：触发计算并返回结果

```mermaid
sequenceDiagram
    participant Client as 客户端代码
    participant RDD
    participant DAGScheduler
    participant TaskScheduler
    participant Executor
    
    Client->>RDD: 转换操作(map, filter等)
    RDD-->>Client: 返回新RDD(不执行计算)
    Client->>RDD: 行动操作(collect, count等)
    RDD->>DAGScheduler: 提交作业
    DAGScheduler->>DAGScheduler: 创建DAG并划分Stage
    DAGScheduler->>TaskScheduler: 提交Stage中的Tasks
    TaskScheduler->>Executor: 分发任务
    Executor->>Executor: 执行任务
    Executor-->>TaskScheduler: 返回结果
    TaskScheduler-->>DAGScheduler: 返回结果
    DAGScheduler-->>RDD: 返回结果
    RDD-->>Client: 返回结果
```

### 1.3 RDD依赖关系

RDD之间的依赖关系分为两种：

1. **窄依赖(Narrow Dependency)**：父RDD的每个分区最多被一个子RDD的分区使用
2. **宽依赖(Wide Dependency)**：父RDD的分区可能被多个子RDD的分区使用，需要Shuffle

```mermaid
graph TD
    subgraph 窄依赖
        A1[父RDD分区1] --> B1[子RDD分区1]
        A2[父RDD分区2] --> B2[子RDD分区2]
        A3[父RDD分区3] --> B3[子RDD分区3]
    end
    
    subgraph 宽依赖
        C1[父RDD分区1] --> D1[子RDD分区1]
        C1 --> D2[子RDD分区2]
        C2[父RDD分区2] --> D1
        C2 --> D2
        C3[父RDD分区3] --> D1
        C3 --> D2
    end
```

### 1.4 Stage划分

DAGScheduler根据RDD的依赖关系将作业划分为多个Stage：

- 在宽依赖处划分Stage
- 每个Stage包含一系列窄依赖的转换操作
- Stage按照依赖关系顺序执行

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

## 2. SQL/DataFrame数据流

### 2.1 SQL查询执行流程

SQL查询的执行流程如下：

```mermaid
graph TD
    A[SQL查询/DataFrame API] --> B[未解析的逻辑计划]
    B --> C[解析器]
    C --> D[已解析的逻辑计划]
    D --> E[分析器]
    E --> F[已分析的逻辑计划]
    F --> G[优化器]
    G --> H[优化后的逻辑计划]
    H --> I[物理计划生成器]
    I --> J[物理执行计划]
    J --> K[代码生成]
    K --> L[RDD执行]
```

### 2.2 Catalyst优化器流程

Catalyst优化器是Spark SQL的核心组件，负责优化查询计划：

1. **解析(Parse)**：将SQL文本解析为未解析的逻辑计划
2. **分析(Analyze)**：解析引用和数据类型
3. **优化(Optimize)**：应用基于规则的优化
4. **物理计划(Physical Planning)**：生成物理执行计划

```mermaid
sequenceDiagram
    participant SQL as SQL查询
    participant Parser as 解析器
    participant Analyzer as 分析器
    participant Optimizer as 优化器
    participant Planner as 物理计划生成器
    participant Execution as 执行引擎
    
    SQL->>Parser: SQL文本
    Parser->>Analyzer: 未解析的逻辑计划
    Analyzer->>Optimizer: 已解析的逻辑计划
    Optimizer->>Planner: 优化后的逻辑计划
    Planner->>Execution: 物理执行计划
    Execution->>SQL: 结果
```

### 2.3 优化规则

Catalyst优化器应用多种优化规则：

1. **谓词下推(Predicate Pushdown)**：将过滤条件尽早应用
2. **列剪裁(Column Pruning)**：只读取需要的列
3. **常量折叠(Constant Folding)**：预计算常量表达式
4. **连接重排序(Join Reordering)**：优化连接顺序
5. **分区裁剪(Partition Pruning)**：只读取需要的分区

### 2.4 代码生成

Spark SQL使用Whole-Stage Code Generation技术优化执行：

1. 将多个操作符融合为单个函数
2. 减少虚函数调用和对象创建
3. 利用CPU缓存和寄存器
4. 减少内存访问

```mermaid
graph TD
    A[物理计划] --> B[代码生成]
    B --> C[Java源代码]
    C --> D[编译]
    D --> E[字节码]
    E --> F[执行]
```

## 3. 流处理数据流

### 3.1 Spark Streaming (DStream)

Spark Streaming将流处理转换为一系列小批量处理：

```mermaid
graph TD
    A[输入流] --> B[接收器]
    B --> C[数据块]
    C --> D[RDD 1]
    C --> E[RDD 2]
    C --> F[RDD 3]
    D --> G[处理]
    E --> G
    F --> G
    G --> H[输出]
    
    subgraph DStream
        D
        E
        F
    end
```

### 3.2 Structured Streaming

Structured Streaming是基于Spark SQL引擎的流处理模型：

```mermaid
graph TD
    A[输入流] --> B[输入表]
    B --> C[查询]
    C --> D[结果表]
    D --> E[输出接收器]
    
    F[触发器] --> C
    G[Checkpoint] --> C
    C --> G
```

处理模式：

1. **微批处理(Micro-Batch Processing)**：定期处理小批量数据
2. **连续处理(Continuous Processing)**：低延迟处理每条记录

## 4. 数据读取流程

### 4.1 文件读取流程

```mermaid
sequenceDiagram
    participant Client as 客户端代码
    participant Context as SparkContext/Session
    participant InputFormat as InputFormat
    participant RecordReader as RecordReader
    participant RDD as RDD/DataFrame
    
    Client->>Context: textFile/read.csv等
    Context->>InputFormat: 获取文件分片
    InputFormat-->>Context: 返回文件分片
    Context->>RDD: 创建RDD/DataFrame
    Client->>RDD: 行动操作
    RDD->>RecordReader: 读取数据
    RecordReader-->>RDD: 返回记录
    RDD-->>Client: 返回结果
```

### 4.2 数据源API

Spark SQL提供了DataSource API，支持多种数据源：

1. **内置数据源**：
   - CSV
   - JSON
   - Parquet
   - ORC
   - JDBC

2. **自定义数据源**：
   - 实现DataSourceV2接口
   - 提供读写能力

```mermaid
graph TD
    A[SparkSession] --> B[DataFrameReader]
    B --> C[DataSource]
    C --> D[FileFormat/JDBCRelation等]
    D --> E[扫描/读取数据]
    E --> F[DataFrame]
```

## 5. 结果输出流程

### 5.1 RDD结果输出

```mermaid
sequenceDiagram
    participant Client as 客户端代码
    participant RDD
    participant DAGScheduler
    participant TaskScheduler
    participant Executor
    
    Client->>RDD: collect/save等
    RDD->>DAGScheduler: 提交作业
    DAGScheduler->>TaskScheduler: 提交任务
    TaskScheduler->>Executor: 执行任务
    Executor->>Executor: 处理数据
    alt 返回结果
        Executor-->>Client: 返回结果集合
    else 保存到存储
        Executor->>存储系统: 写入数据
    end
```

### 5.2 DataFrame/Dataset结果输出

```mermaid
graph TD
    A[DataFrame/Dataset] --> B[DataFrameWriter]
    B --> C[保存模式选择]
    C --> D[格式选择]
    D --> E[输出路径/表名]
    E --> F[执行写入]
```

保存模式：

1. **error**：如果数据已存在则报错（默认）
2. **append**：追加到现有数据
3. **overwrite**：覆盖现有数据
4. **ignore**：如果数据已存在则忽略

### 5.3 流处理结果输出

```mermaid
graph TD
    A[DStream/结构化流] --> B[输出操作]
    B --> C[输出模式选择]
    
    C --> D[完整模式]
    C --> E[追加模式]
    C --> F[更新模式]
    
    D --> G[输出接收器]
    E --> G
    F --> G
    
    G --> H[文件系统]
    G --> I[数据库]
    G --> J[消息队列]
```

输出模式：

1. **完整模式(Complete Mode)**：输出整个结果表
2. **追加模式(Append Mode)**：只输出新增的行
3. **更新模式(Update Mode)**：只输出更新的行

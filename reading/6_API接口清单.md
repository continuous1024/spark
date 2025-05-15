# Apache Spark API接口清单

## Spark Core API

### SparkContext API

- **创建RDD**
    - `parallelize(seq)`: 从本地集合创建RDD
    - `textFile(path)`: 从文本文件创建RDD
    - `wholeTextFiles(path)`: 读取整个文本文件
    - `sequenceFile(path)`: 读取Hadoop序列文件
    - `hadoopRDD(conf, inputClass, valueClass, path)`: 从Hadoop输入格式创建RDD
    - `newAPIHadoopRDD(conf, inputClass, valueClass, path)`: 从新版Hadoop API创建RDD

- **配置**
    - `setJobGroup(groupId, description)`: 设置作业组
    - `setLocalProperty(key, value)`: 设置本地属性
    - `setLogLevel(level)`: 设置日志级别
    - `setCheckpointDir(directory)`: 设置检查点目录

- **资源管理**
    - `addFile(path)`: 添加文件到所有节点
    - `addJar(path)`: 添加JAR到所有节点
    - `broadcast(value)`: 创建广播变量
    - `accumulator(initialValue)`: 创建累加器

### RDD API

- **转换操作**
    - `map(func)`: 对每个元素应用函数
    - `flatMap(func)`: 对每个元素应用函数并展平结果
    - `filter(func)`: 过滤元素
    - `distinct()`: 去重
    - `sample(withReplacement, fraction, seed)`: 采样
    - `union(otherRDD)`: 合并RDD
    - `intersection(otherRDD)`: 求交集
    - `subtract(otherRDD)`: 求差集
    - `cartesian(otherRDD)`: 笛卡尔积
    - `groupByKey()`: 按键分组
    - `reduceByKey(func)`: 按键归约
    - `aggregateByKey(zeroValue)(seqFunc, combFunc)`: 按键聚合
    - `sortByKey()`: 按键排序
    - `join(otherRDD)`: 连接RDD
    - `cogroup(otherRDD)`: 协同分组
    - `pipe(command)`: 通过外部命令处理RDD

- **行动操作**
    - `collect()`: 收集所有元素
    - `count()`: 计数
    - `first()`: 获取第一个元素
    - `take(n)`: 获取前n个元素
    - `takeSample(withReplacement, n, seed)`: 采样n个元素
    - `takeOrdered(n, [ordering])`: 获取排序后的前n个元素
    - `saveAsTextFile(path)`: 保存为文本文件
    - `saveAsSequenceFile(path)`: 保存为序列文件
    - `saveAsObjectFile(path)`: 保存为对象文件
    - `countByKey()`: 按键计数
    - `foreach(func)`: 对每个元素执行函数

- **持久化**
    - `cache()`: 缓存RDD
    - `persist(storageLevel)`: 持久化RDD
    - `unpersist()`: 移除持久化数据
    - `checkpoint()`: 检查点RDD

## Spark SQL API

### SparkSession API

- **创建DataFrame**
    - `read`: 返回DataFrameReader
    - `createDataFrame(data, schema)`: 从数据创建DataFrame
    - `table(tableName)`: 从表创建DataFrame
    - `sql(query)`: 执行SQL查询

- **配置**
    - `conf`: 访问配置
    - `udf`: 访问UDF注册接口
    - `catalog`: 访问目录

### DataFrame/Dataset API

- **转换操作**
    - `select(cols)`: 选择列
    - `filter(condition)`: 过滤行
    - `where(condition)`: 过滤行（同filter）
    - `groupBy(cols)`: 分组
    - `agg(exprs)`: 聚合
    - `join(otherDF, joinExprs, joinType)`: 连接
    - `union(otherDF)`: 合并
    - `orderBy(cols)`: 排序
    - `sort(cols)`: 排序（同orderBy）
    - `limit(n)`: 限制结果数量
    - `distinct()`: 去重
    - `dropDuplicates(cols)`: 按列去重
    - `withColumn(colName, expr)`: 添加列
    - `withColumnRenamed(existingName, newName)`: 重命名列
    - `drop(colName)`: 删除列
    - `sample(fraction, seed)`: 采样
    - `randomSplit(weights, seed)`: 随机分割

- **行动操作**
    - `show(n, truncate)`: 显示数据
    - `collect()`: 收集所有行
    - `count()`: 计数
    - `first()`: 获取第一行
    - `head(n)`: 获取前n行
    - `take(n)`: 获取前n行（同head）
    - `describe(cols)`: 统计描述
    - `summary(cols)`: 统计摘要
    - `write`: 返回DataFrameWriter

- **其他操作**
    - `cache()`: 缓存
    - `persist(storageLevel)`: 持久化
    - `unpersist()`: 移除持久化数据
    - `createOrReplaceTempView(viewName)`: 创建临时视图
    - `createGlobalTempView(viewName)`: 创建全局临时视图
    - `explain(extended)`: 解释查询计划
    - `printSchema()`: 打印模式

### DataFrameReader API

- `format(source)`: 设置数据源格式
- `schema(schema)`: 设置模式
- `option(key, value)`: 设置选项
- `options(map)`: 设置多个选项
- `load(path)`: 加载数据
- `json(path)`: 加载JSON
- `csv(path)`: 加载CSV
- `parquet(path)`: 加载Parquet
- `orc(path)`: 加载ORC
- `text(path)`: 加载文本
- `table(tableName)`: 加载表

### DataFrameWriter API

- `format(source)`: 设置数据源格式
- `mode(saveMode)`: 设置保存模式
- `option(key, value)`: 设置选项
- `options(map)`: 设置多个选项
- `partitionBy(colNames)`: 按列分区
- `bucketBy(numBuckets, colName)`: 按列分桶
- `sortBy(colNames)`: 排序
- `save(path)`: 保存数据
- `json(path)`: 保存为JSON
- `csv(path)`: 保存为CSV
- `parquet(path)`: 保存为Parquet
- `orc(path)`: 保存为ORC
- `text(path)`: 保存为文本
- `saveAsTable(tableName)`: 保存为表
- `insertInto(tableName)`: 插入到表中

## Structured Streaming API

### DataStreamReader API

- `format(source)`: 设置数据源格式
- `schema(schema)`: 设置模式
- `option(key, value)`: 设置选项
- `options(map)`: 设置多个选项
- `load(path)`: 加载流数据
- `json(path)`: 加载JSON流
- `csv(path)`: 加载CSV流
- `parquet(path)`: 加载Parquet流
- `orc(path)`: 加载ORC流
- `text(path)`: 加载文本流
- `socket(host, port)`: 从套接字读取

### DataStreamWriter API

- `format(source)`: 设置接收器格式
- `outputMode(outputMode)`: 设置输出模式
- `option(key, value)`: 设置选项
- `options(map)`: 设置多个选项
- `partitionBy(colNames)`: 按列分区
- `queryName(queryName)`: 设置查询名称
- `trigger(trigger)`: 设置触发器
- `foreachBatch(function)`: 批处理函数
- `foreach(writer)`: 自定义写入器
- `start(path)`: 启动查询
- `toTable(tableName)`: 写入表

### StreamingQuery API

- `id()`: 获取查询ID
- `runId()`: 获取运行ID
- `name()`: 获取查询名称
- `explain()`: 解释查询计划
- `stop()`: 停止查询
- `awaitTermination()`: 等待终止
- `exception()`: 获取异常
- `status()`: 获取状态
- `lastProgress()`: 获取最后进度
- `recentProgress()`: 获取最近进度
- `processAllAvailable()`: 处理所有可用数据

## MLlib API

### Pipeline API

- `Pipeline()`: 创建管道
- `PipelineModel`: 管道模型
- `Transformer`: 转换器接口
- `Estimator`: 估计器接口
- `Evaluator`: 评估器接口
- `Param`: 参数接口

### 特征处理

- `VectorAssembler`: 向量组装器
- `StringIndexer`: 字符串索引器
- `OneHotEncoder`: 独热编码器
- `Tokenizer`: 分词器
- `StopWordsRemover`: 停用词移除器
- `Word2Vec`: 词向量
- `CountVectorizer`: 计数向量器
- `IDF`: 逆文档频率
- `Normalizer`: 归一化器
- `StandardScaler`: 标准化器
- `MinMaxScaler`: 最小最大缩放器
- `Bucketizer`: 分桶器
- `PCA`: 主成分分析

### 分类和回归

- `LogisticRegression`: 逻辑回归
- `DecisionTreeClassifier`: 决策树分类器
- `RandomForestClassifier`: 随机森林分类器
- `GBTClassifier`: 梯度提升树分类器
- `LinearRegression`: 线性回归
- `DecisionTreeRegressor`: 决策树回归器
- `RandomForestRegressor`: 随机森林回归器
- `GBTRegressor`: 梯度提升树回归器

### 聚类

- `KMeans`: K均值聚类
- `BisectingKMeans`: 二分K均值聚类
- `GaussianMixture`: 高斯混合模型
- `LDA`: 潜在狄利克雷分配

### 评估

- `BinaryClassificationEvaluator`: 二分类评估器
- `MulticlassClassificationEvaluator`: 多分类评估器
- `RegressionEvaluator`: 回归评估器
- `ClusteringEvaluator`: 聚类评估器

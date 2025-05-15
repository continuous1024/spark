# Spark API接口清单

本文档详细介绍了Apache Spark提供的各种编程接口，包括Scala、Java、Python和R语言的API。

## 1. Scala API

Scala是Spark的主要开发语言，提供了最完整和最原生的API。

### 1.1 Core API

#### SparkContext

```scala
// 创建SparkContext
val conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
val sc = new SparkContext(conf)

// 创建RDD
val rdd1 = sc.parallelize(1 to 100)
val rdd2 = sc.textFile("data.txt")
val rdd3 = sc.wholeTextFiles("data-dir")

// 广播变量和累加器
val broadcast = sc.broadcast(Map("key" -> "value"))
val accumulator = sc.longAccumulator("myAccumulator")
```

#### RDD操作

```scala
// 转换操作
val mapped = rdd.map(_ * 2)
val filtered = rdd.filter(_ > 10)
val flatMapped = rdd.flatMap(x => x.to(x + 10))
val paired = rdd.map(x => (x, x * x))
val grouped = paired.groupByKey()
val reduced = paired.reduceByKey(_ + _)
val joined = rdd1.join(rdd2)

// 行动操作
val count = rdd.count()
val collected = rdd.collect()
val first = rdd.first()
val taken = rdd.take(10)
val reduced = rdd.reduce(_ + _)
val saved = rdd.saveAsTextFile("output")
```

### 1.2 SQL API

#### SparkSession

```scala
// 创建SparkSession
val spark = SparkSession.builder()
  .appName("MyApp")
  .master("local[*]")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

// 从文件创建DataFrame
val df1 = spark.read.json("people.json")
val df2 = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("data.csv")

// 从表创建DataFrame
val df3 = spark.table("my_table")

// 执行SQL
val results = spark.sql("SELECT * FROM people WHERE age > 21")
```

#### DataFrame操作

```scala
// 基本操作
df.select("name", "age")
df.filter($"age" > 21)
df.groupBy("age").count()
df.join(df2, $"df1.id" === $"df2.id")
df.orderBy($"age".desc)
df.limit(10)

// 聚合操作
import org.apache.spark.sql.functions._
df.select(avg($"salary"), max($"age"))
df.groupBy($"department").agg(avg($"salary"), max($"age"))

// 写入数据
df.write.format("parquet").save("output")
df.write.mode("overwrite").saveAsTable("my_table")
```

#### Dataset API

```scala
// 定义case class
case class Person(name: String, age: Int)

// 创建Dataset
val ds = spark.read.json("people.json").as[Person]

// 类型安全操作
ds.filter(_.age > 21)
ds.map(p => p.name.toUpperCase)
ds.groupByKey(_.age).count()
```

### 1.3 Streaming API

#### Spark Streaming (DStream)

```scala
// 创建StreamingContext
val ssc = new StreamingContext(sc, Seconds(1))

// 创建DStream
val lines = ssc.socketTextStream("localhost", 9999)
val words = lines.flatMap(_.split(" "))
val pairs = words.map(word => (word, 1))
val wordCounts = pairs.reduceByKey(_ + _)

// 输出操作
wordCounts.print()
wordCounts.saveAsTextFiles("output")

// 启动流处理
ssc.start()
ssc.awaitTermination()
```

#### Structured Streaming

```scala
// 创建输入流
val streamingDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host:port")
  .option("subscribe", "topic")
  .load()

// 处理数据
val query = streamingDF
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .groupBy($"key")
  .count()
  .writeStream
  .outputMode("complete")
  .format("console")
  .start()

// 等待查询终止
query.awaitTermination()
```

### 1.4 MLlib API

```scala
// 创建训练数据
val data = spark.read.format("libsvm").load("sample_data.txt")

// 特征处理
val assembler = new VectorAssembler()
  .setInputCols(Array("feature1", "feature2", "feature3"))
  .setOutputCol("features")

// 创建和训练模型
val lr = new LogisticRegression()
  .setMaxIter(10)
  .setRegParam(0.01)

// 创建Pipeline
val pipeline = new Pipeline().setStages(Array(assembler, lr))
val model = pipeline.fit(data)

// 预测
val predictions = model.transform(testData)
```

## 2. Java API

Java API与Scala API类似，但使用Java集合和接口。

### 2.1 Core API

#### JavaSparkContext

```java
// 创建JavaSparkContext
SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]");
JavaSparkContext jsc = new JavaSparkContext(conf);

// 创建JavaRDD
JavaRDD<Integer> rdd1 = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
JavaRDD<String> rdd2 = jsc.textFile("data.txt");
```

#### JavaRDD操作

```java
// 转换操作
JavaRDD<Integer> mapped = rdd.map(x -> x * 2);
JavaRDD<Integer> filtered = rdd.filter(x -> x > 10);
JavaPairRDD<String, Integer> paired = rdd.mapToPair(s -> new Tuple2<>(s, 1));
JavaPairRDD<String, Integer> reduced = paired.reduceByKey((a, b) -> a + b);

// 行动操作
long count = rdd.count();
List<Integer> collected = rdd.collect();
```

### 2.2 SQL API

#### SparkSession

```java
// 创建SparkSession
SparkSession spark = SparkSession.builder()
  .appName("MyApp")
  .master("local[*]")
  .getOrCreate();

// 创建DataFrame
Dataset<Row> df = spark.read().json("people.json");
```

#### DataFrame操作

```java
// 基本操作
df.select("name", "age");
df.filter(col("age").gt(21));
df.groupBy("age").count();

// 使用SQL
df.createOrReplaceTempView("people");
Dataset<Row> results = spark.sql("SELECT * FROM people WHERE age > 21");
```

## 3. Python API (PySpark)

PySpark提供了Python友好的API，几乎覆盖了Spark的所有功能。

### 3.1 Core API

#### SparkContext

```python
# 创建SparkContext
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("MyApp").setMaster("local[*]")
sc = SparkContext(conf=conf)

# 创建RDD
rdd1 = sc.parallelize([1, 2, 3, 4, 5])
rdd2 = sc.textFile("data.txt")
```

#### RDD操作

```python
# 转换操作
mapped = rdd.map(lambda x: x * 2)
filtered = rdd.filter(lambda x: x > 10)
paired = rdd.map(lambda x: (x, 1))
reduced = paired.reduceByKey(lambda a, b: a + b)

# 行动操作
count = rdd.count()
collected = rdd.collect()
```

### 3.2 SQL API

#### SparkSession

```python
# 创建SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .getOrCreate()

# 创建DataFrame
df1 = spark.read.json("people.json")
df2 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("data.csv")
```

#### DataFrame操作

```python
# 基本操作
from pyspark.sql.functions import col, avg, max
df.select("name", "age")
df.filter(col("age") > 21)
df.groupBy("age").count()

# 使用SQL
df.createOrReplaceTempView("people")
results = spark.sql("SELECT * FROM people WHERE age > 21")
```

### 3.3 Pandas API on Spark

```python
# 导入Pandas API on Spark
import pyspark.pandas as ps

# 创建DataFrame
psdf = ps.read_csv("data.csv")

# 使用Pandas风格的API
filtered = psdf[psdf['age'] > 21]
grouped = psdf.groupby('category').agg({'value': 'mean'})
```

### 3.4 MLlib API

```python
# 导入MLlib组件
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

# 特征处理
assembler = VectorAssembler(
    inputCols=["feature1", "feature2", "feature3"],
    outputCol="features")

# 创建和训练模型
lr = LogisticRegression(maxIter=10, regParam=0.01)
pipeline = Pipeline(stages=[assembler, lr])
model = pipeline.fit(data)
```

## 4. R API (SparkR)

SparkR提供了R语言接口，但功能相对有限，且已被标记为弃用。

### 4.1 基本API

```r
# 初始化SparkSession
library(SparkR)
sparkR.session(appName = "MyApp", master = "local[*]")

# 创建DataFrame
df <- read.df("people.json", "json")

# 基本操作
select(df, "name", "age")
filter(df, df$age > 21)
count(groupBy(df, "age"))

# 使用SQL
createOrReplaceTempView(df, "people")
results <- sql("SELECT * FROM people WHERE age > 21")
```

### 4.2 MLlib集成

```r
# 创建训练数据
training <- read.df("sample_data.csv", "csv", header = "true", inferSchema = "true")

# 创建和训练模型
model <- spark.glm(training, Sepal_Length ~ Sepal_Width + Species, family = "gaussian")

# 预测
predictions <- predict(model, newData)
```

## 5. API兼容性和版本差异

### 5.1 API稳定性

Spark的API稳定性分为三个级别：

1. **稳定(Stable)**：API不会在次要版本中发生破坏性变更
2. **开发者API(Developer API)**：可能在次要版本中发生变更
3. **实验性(Experimental)**：可能随时变更

### 5.2 主要版本间的差异

- **Spark 1.x vs 2.x**：引入DataFrame/Dataset API和SparkSession
- **Spark 2.x vs 3.x**：Python类型提示、Pandas API on Spark、Spark Connect

### 5.3 语言间的差异

- **Scala**：最完整的API，支持所有功能
- **Java**：几乎完整的API，但缺少一些Scala特有功能
- **Python**：大部分功能，但性能可能略低
- **R**：功能有限，已被标记为弃用

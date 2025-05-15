# Spark SQL 组件

Spark SQL是Spark的结构化数据处理模块，它提供了一种高级API，使开发者能够使用SQL或DataFrame API处理结构化数据。本章将详细介绍Spark SQL的核心组件和实现机制。

## 1. SparkSession实现

SparkSession是Spark SQL的入口点，它提供了一个统一的接口来使用Spark的各种功能。

### 1.1 SparkSession的核心结构

SparkSession定义在`org.apache.spark.sql.SparkSession`类中：

```scala
class SparkSession private(
    @transient val sparkContext: SparkContext,
    @transient private val existingSharedState: Option[SharedState],
    @transient private val parentSessionState: Option[SessionState],
    @transient private[sql] val extensions: SparkSessionExtensions,
    @transient private[sql] val initialSessionOptions: Map[String, String])
  extends Serializable with Closeable with Logging { self =>

  // 会话状态
  @transient
  lazy val sessionState: SessionState = {
    parentSessionState.getOrElse {
      SparkSession.instantiateSessionState(
        SparkSession.sessionStateClassName(sparkContext.conf),
        self)
    }
  }

  // 共享状态
  @transient
  lazy val sharedState: SharedState = {
    existingSharedState.getOrElse {
      new SharedState(sparkContext)
    }
  }

  // 目录
  @transient
  lazy val catalog: Catalog = new CatalogImpl(this)

  // UDF注册表
  @transient
  lazy val udf: UDFRegistration = new UDFRegistration(this)

  // 创建DataFrame
  def createDataFrame(data: RDD[Row], schema: StructType): DataFrame = {
    Dataset.ofRows(self, sessionState.executePlan(
      LogicalRDD(schema.toAttributes, data, isStreaming = false)(self)))
  }

  // 创建Dataset
  def createDataset[T: Encoder](data: Seq[T]): Dataset[T] = {
    val enc = encoderFor[T]
    val toRow = enc.createSerializer()
    val attributes = enc.schema.toAttributes
    val encoded = data.map(d => toRow(d))
    val plan = LocalRelation(attributes, encoded)
    Dataset(self, plan)
  }

  // 执行SQL
  def sql(sqlText: String): DataFrame = {
    Dataset.ofRows(self, sessionState.sqlParser.parsePlan(sqlText))
  }

  // 其他方法...
}
```

### 1.2 SparkSession的构建器

SparkSession使用构建器模式创建实例：

```scala
object SparkSession {
  // 构建器
  class Builder {
    private[this] val options = new scala.collection.mutable.HashMap[String, String]
    private[this] var userSuppliedContext: Option[SparkContext] = None
    private[this] var extensions: SparkSessionExtensions = new SparkSessionExtensions

    // 设置主URL
    def master(master: String): Builder = {
      options += ("spark.master" -> master)
      this
    }

    // 设置应用名称
    def appName(name: String): Builder = {
      options += ("spark.app.name" -> name)
      this
    }

    // 设置配置
    def config(key: String, value: String): Builder = {
      options += (key -> value)
      this
    }

    // 设置SparkContext
    def sparkContext(sparkContext: SparkContext): Builder = {
      userSuppliedContext = Option(sparkContext)
      this
    }

    // 启用Hive支持
    def enableHiveSupport(): Builder = {
      config("spark.sql.catalogImplementation", "hive")
      this
    }

    // 构建SparkSession
    def getOrCreate(): SparkSession = synchronized {
      // 获取活动的SparkSession
      val session = activeThreadSession.get()
      if (session != null) {
        return session
      }

      // 获取默认的SparkSession
      val sparkContext = userSuppliedContext.getOrElse {
        val sparkConf = new SparkConf()
        options.foreach { case (k, v) => sparkConf.set(k, v) }

        // 如果已经有SparkContext，使用它
        if (SparkContext.getActive.isDefined) {
          SparkContext.getActive.get
        } else {
          // 创建新的SparkContext
          new SparkContext(sparkConf)
        }
      }

      // 创建SparkSession
      val session = new SparkSession(sparkContext, None, None, extensions, options.toMap)

      // 设置默认会话
      SparkSession.setDefaultSession(session)

      // 设置活动会话
      SparkSession.setActiveSession(session)

      // 返回会话
      session
    }
  }

  // 创建构建器
  def builder(): Builder = new Builder

  // 其他方法...
}
```

## 2. DataFrame/Dataset实现

DataFrame和Dataset是Spark SQL的核心数据抽象，它们提供了一种类型安全的、结构化的数据处理API。

### 2.1 Dataset的核心结构

Dataset定义在`org.apache.spark.sql.Dataset`类中：

```scala
class Dataset[T] private[sql](
    @transient val sparkSession: SparkSession,
    @DeveloperApi @transient val queryExecution: QueryExecution,
    @DeveloperApi @transient val encoder: Encoder[T])
  extends Serializable {

  // 逻辑计划
  @transient private[sql] val logicalPlan: LogicalPlan = queryExecution.analyzed

  // 类型标签
  private[sql] val classTag = encoder.clsTag

  // 创建新的Dataset
  private[sql] def copy(
      newQueryExecution: QueryExecution = queryExecution,
      newEncoder: Encoder[T] = encoder): Dataset[T] = {
    new Dataset(sparkSession, newQueryExecution, newEncoder)
  }

  // 转换操作
  def map[U: Encoder](func: T => U): Dataset[U] = {
    val encoder = encoderFor[U]
    val toRow = encoder.createSerializer()
    val fromRow = this.encoder.createDeserializer()
    val mapped = sparkSession.sessionState.executePlan(
      MapElements(func.asInstanceOf[Any => Any], logicalPlan, ClassTag(encoder.clsTag.runtimeClass)))
    new Dataset(sparkSession, mapped, encoder)
  }

  // 过滤操作
  def filter(condition: T => Boolean): Dataset[T] = {
    val func = condition.asInstanceOf[Any => Boolean]
    val filtered = sparkSession.sessionState.executePlan(
      Filter(func, logicalPlan))
    new Dataset(sparkSession, filtered, encoder)
  }

  // 聚合操作
  def groupBy(cols: Column*): RelationalGroupedDataset = {
    val groupingExprs = cols.map(_.expr).toSeq
    new RelationalGroupedDataset(
      sparkSession,
      logicalPlan,
      groupingExprs,
      RelationalGroupedDataset.GroupByType)
  }

  // 连接操作
  def join(right: Dataset[_], joinExprs: Column, joinType: String): DataFrame = {
    val joinTypeObj = JoinType(joinType)
    val joined = sparkSession.sessionState.executePlan(
      Join(logicalPlan, right.logicalPlan, joinTypeObj, Some(joinExprs.expr)))
    new Dataset(sparkSession, joined, RowEncoder(joined.schema))
  }

  // 行动操作
  def collect(): Array[T] = {
    val result = queryExecution.executedPlan.executeCollect()
    val fromRow = encoder.createDeserializer()
    result.map(fromRow)
  }

  // 其他方法...
}
```

### 2.2 DataFrame的实现

DataFrame是Dataset[Row]的别名，它提供了一种无类型的数据处理API：

```scala
object Dataset {
  // 创建DataFrame
  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
  }

  // 其他方法...
}

// DataFrame类型别名
type DataFrame = Dataset[Row]
```

### 2.3 编码器

编码器是Dataset的核心组件，它负责在JVM对象和Spark内部表示之间进行转换：

```scala
trait Encoder[T] extends Serializable {
  // 模式
  def schema: StructType

  // 类型标签
  def clsTag: ClassTag[T]

  // 创建序列化器
  def createSerializer(): Any => InternalRow

  // 创建反序列化器
  def createDeserializer(): InternalRow => T
}

// 产品编码器
class ProductEncoder[T <: Product: TypeTag] extends Encoder[T] {
  // 获取模式
  override def schema: StructType = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

  // 获取类型标签
  override def clsTag: ClassTag[T] = ClassTag[T](typeTag[T].mirror.runtimeClass(typeTag[T].tpe))

  // 创建序列化器
  override def createSerializer(): Any => InternalRow = {
    val inputObject = BoundReference(0, ObjectType(clsTag.runtimeClass), nullable = true)
    val serializer = ScalaReflection.serializerFor[T](inputObject)
    (obj: Any) => {
      serializer.eval(obj.asInstanceOf[T])
    }
  }

  // 创建反序列化器
  override def createDeserializer(): InternalRow => T = {
    val deserializer = ScalaReflection.deserializerFor[T]
    (row: InternalRow) => {
      deserializer.eval(row).asInstanceOf[T]
    }
  }
}

// 行编码器
case class RowEncoder(schema: StructType) extends Encoder[Row] {
  // 获取类型标签
  override def clsTag: ClassTag[Row] = ClassTag(classOf[Row])

  // 创建序列化器
  override def createSerializer(): Any => InternalRow = {
    val serializer = RowSerializer(schema.map(_.dataType))
    (obj: Any) => {
      serializer.serialize(obj.asInstanceOf[Row])
    }
  }

  // 创建反序列化器
  override def createDeserializer(): InternalRow => Row = {
    val deserializer = RowDeserializer(schema.map(_.dataType))
    (row: InternalRow) => {
      deserializer.deserialize(row)
    }
  }
}
```

## 3. 查询执行引擎

Spark SQL的查询执行引擎负责将逻辑计划转换为物理计划，并在集群上执行查询。

### 3.1 QueryExecution的核心结构

QueryExecution定义在`org.apache.spark.sql.execution.QueryExecution`类中：

```scala
class QueryExecution(
    val sparkSession: SparkSession,
    val logical: LogicalPlan,
    val tracker: QueryPlanningTracker = new QueryPlanningTracker)
  extends Logging {

  // 分析器
  lazy val analyzed: LogicalPlan = {
    SparkSession.setActiveSession(sparkSession)
    sparkSession.sessionState.analyzer.execute(logical)
  }

  // 优化器
  lazy val optimizedPlan: LogicalPlan = {
    SparkSession.setActiveSession(sparkSession)
    sparkSession.sessionState.optimizer.execute(analyzed)
  }

  // 物理计划
  lazy val sparkPlan: SparkPlan = {
    SparkSession.setActiveSession(sparkSession)
    sparkSession.sessionState.planner.plan(optimizedPlan).next()
  }

  // 准备执行
  lazy val executedPlan: SparkPlan = {
    SparkSession.setActiveSession(sparkSession)
    sparkSession.sessionState.prepareForExecution.execute(sparkPlan)
  }

  // 执行收集
  def executeCollect(): Array[InternalRow] = {
    executedPlan.executeCollect()
  }

  // 执行收集限制
  def executeCollectLimit(limit: Int): Array[InternalRow] = {
    executedPlan.executeCollectLimit(limit)
  }

  // 执行取第一行
  def executeTake(limit: Int): Array[InternalRow] = {
    executedPlan.executeTake(limit)
  }

  // 其他方法...
}
```

### 3.2 执行计划的生成过程

Spark SQL的执行计划生成过程包括以下步骤：

1. **解析**：将SQL文本解析为未解析的逻辑计划
2. **分析**：解析引用和表达式，生成已解析的逻辑计划
3. **优化**：应用优化规则，生成优化后的逻辑计划
4. **物理计划生成**：将逻辑计划转换为物理计划
5. **准备执行**：应用物理优化规则，生成最终的执行计划

```scala
// SQL解析
val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(sqlText)

// 分析
val analyzedPlan = sparkSession.sessionState.analyzer.execute(logicalPlan)

// 优化
val optimizedPlan = sparkSession.sessionState.optimizer.execute(analyzedPlan)

// 物理计划生成
val physicalPlan = sparkSession.sessionState.planner.plan(optimizedPlan).next()

// 准备执行
val executedPlan = sparkSession.sessionState.prepareForExecution.execute(physicalPlan)

// 执行
val result = executedPlan.executeCollect()
```

## 4. SQL解析器

SQL解析器负责将SQL文本解析为未解析的逻辑计划。Spark SQL使用ANTLR4作为解析器生成工具。

### 4.1 SqlParser的核心结构

SqlParser定义在`org.apache.spark.sql.catalyst.parser.AbstractSqlParser`抽象类中：

```scala
abstract class AbstractSqlParser extends ParserInterface {
  // 解析计划
  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    astBuilder.visitSingleStatement(parser.singleStatement()) match {
      case plan: LogicalPlan => plan
      case _ => throw new ParseException("Unsupported SQL statement", null)
    }
  }

  // 解析表达式
  override def parseExpression(sqlText: String): Expression = parse(sqlText) { parser =>
    astBuilder.visitSingleExpression(parser.singleExpression()) match {
      case expression: Expression => expression
      case _ => throw new ParseException("Unsupported SQL expression", null)
    }
  }

  // 解析表标识符
  override def parseTableIdentifier(sqlText: String): TableIdentifier = parse(sqlText) { parser =>
    astBuilder.visitSingleTableIdentifier(parser.singleTableIdentifier()) match {
      case tableIdentifier: TableIdentifier => tableIdentifier
      case _ => throw new ParseException("Unsupported SQL table identifier", null)
    }
  }

  // 解析数据类型
  override def parseDataType(sqlText: String): DataType = parse(sqlText) { parser =>
    astBuilder.visitSingleDataType(parser.singleDataType()) match {
      case dataType: DataType => dataType
      case _ => throw new ParseException("Unsupported SQL data type", null)
    }
  }

  // 解析方法
  protected def parse[T](sqlText: String)(toResult: SqlBaseParser => T): T = {
    val lexer = new SqlBaseLexer(new ANTLRNoCaseStringStream(sqlText))
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)

    try {
      toResult(parser)
    } catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(sqlText)
      case e: AnalysisException =>
        throw e
    }
  }

  // 其他方法...
}
```

### 4.2 AstBuilder的实现

AstBuilder负责将ANTLR生成的解析树转换为逻辑计划：

```scala
class AstBuilder extends SqlBaseBaseVisitor[AnyRef] {
  // 访问SELECT语句
  override def visitQuerySpecification(ctx: QuerySpecificationContext): LogicalPlan = {
    // 解析FROM子句
    val from = OneRowRelation

    // 解析WHERE子句
    val where = Option(ctx.where)
      .map(visitBooleanExpression)
      .map(Filter(_, from))
      .getOrElse(from)

    // 解析GROUP BY子句
    val groupBy = Option(ctx.groupBy)
      .map(visitGroupBy)
      .map(Aggregate(_, Seq.empty, where))
      .getOrElse(where)

    // 解析HAVING子句
    val having = Option(ctx.having)
      .map(visitBooleanExpression)
      .map(Filter(_, groupBy))
      .getOrElse(groupBy)

    // 解析SELECT子句
    val select = Option(ctx.selectClause)
      .map(visitSelectClause)
      .map(Project(_, having))
      .getOrElse(having)

    // 解析DISTINCT子句
    val distinct = if (ctx.setQuantifier() != null && ctx.setQuantifier().DISTINCT() != null) {
      Distinct(select)
    } else {
      select
    }

    // 解析ORDER BY子句
    val sort = Option(ctx.sortClause)
      .map(visitSortClause)
      .map(Sort(_, distinct))
      .getOrElse(distinct)

    // 解析LIMIT子句
    val limit = Option(ctx.limitClause)
      .map(visitLimitClause)
      .map(Limit(_, sort))
      .getOrElse(sort)

    limit
  }

  // 其他访问方法...
}
```

## 5. 自适应查询执行 (AQE)

自适应查询执行是Spark SQL的一项高级优化技术，它在运行时根据统计信息动态调整查询计划。

### 5.1 AQE的核心结构

AQE定义在`org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec`类中：

```scala
case class AdaptiveSparkPlanExec(
    inputPlan: SparkPlan,
    @transient context: AdaptiveExecutionContext,
    @transient preprocessingRules: Seq[Rule[SparkPlan]],
    @transient queryStageOptimizerRules: Seq[Rule[SparkPlan]],
    @transient queryStagePreparationRules: Seq[Rule[SparkPlan]],
    @transient costEvaluator: CostEvaluator)
  extends LeafExecNode {

  // 输出属性
  override def output: Seq[Attribute] = finalPlan.output

  // 最终计划
  @transient private[sql] var finalPlan: SparkPlan = _

  // 执行
  override protected def doExecute(): RDD[InternalRow] = {
    // 获取最终计划
    val executedPlan = getFinalPhysicalPlan()

    // 执行最终计划
    executedPlan.execute()
  }

  // 获取最终物理计划
  def getFinalPhysicalPlan(): SparkPlan = {
    // 如果已经有最终计划，直接返回
    if (finalPlan != null) {
      return finalPlan
    }

    // 创建初始计划
    val initialPlan = applyPhysicalRules(
      inputPlan, preprocessingRules, context.session, context.adaptiveExecutionContext)

    // 创建查询阶段
    val queryStages = createQueryStages(initialPlan)

    // 等待查询阶段完成
    val finalStages = queryStages.map { stage =>
      stage.resultOption.getOrElse {
        stage.executeIfNeeded()
        stage
      }
    }

    // 优化查询阶段
    val optimizedPlan = applyPhysicalRules(
      replaceWithQueryStagesInLogicalPlan(finalStages),
      queryStageOptimizerRules,
      context.session,
      context.adaptiveExecutionContext)

    // 准备最终计划
    finalPlan = applyPhysicalRules(
      optimizedPlan,
      queryStagePreparationRules,
      context.session,
      context.adaptiveExecutionContext)

    // 返回最终计划
    finalPlan
  }

  // 其他方法...
}
```

### 5.2 AQE的优化规则

AQE包含多种优化规则，用于在运行时优化查询计划：

```scala
// 合并Shuffle分区
case class CoalesceShufflePartitions(
    maxPartitionBytes: Long,
    minPartitionNum: Int,
    advisoryPartitionSize: Long)
  extends Rule[SparkPlan] {

  // 应用规则
  override def apply(plan: SparkPlan): SparkPlan = {
    // 查找Shuffle读取
    val shuffleReads = collectShuffleReads(plan)

    // 如果没有Shuffle读取，返回原计划
    if (shuffleReads.isEmpty) {
      return plan
    }

    // 获取Shuffle统计信息
    val shuffleStageInfos = shuffleReads.map { read =>
      val mapStats = read.mapStats
      val bytesByPartition = mapStats.bytesByPartitionId
      val partitionSize = bytesByPartition.sum
      val partitionNum = bytesByPartition.length

      ShuffleStageInfo(read, partitionSize, partitionNum)
    }

    // 计算目标分区数
    val targetPartitionNum = shuffleStageInfos.map { info =>
      val targetPartitionNum = math.max(
        minPartitionNum,
        math.ceil(info.partitionSize / maxPartitionBytes).toInt)

      (info.read, targetPartitionNum)
    }.toMap

    // 替换计划
    plan.transformUp {
      case read: ShuffleQueryStageExec if targetPartitionNum.contains(read) =>
        val newPartitionNum = targetPartitionNum(read)

        if (newPartitionNum < read.partitionNum) {
          // 创建合并Shuffle分区操作
          CoalescedShuffleReaderExec(read, newPartitionNum)
        } else {
          read
        }
    }
  }

  // 其他方法...
}

// 动态连接选择
case class DynamicJoinSelection(conf: SQLConf) extends Rule[SparkPlan] {
  // 应用规则
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case join: SortMergeJoinExec =>
        // 获取左右两侧的大小
        val leftSize = join.left.stats.sizeInBytes
        val rightSize = join.right.stats.sizeInBytes

        // 如果一侧足够小，使用广播连接
        if (leftSize < conf.autoBroadcastJoinThreshold) {
          BroadcastHashJoinExec(
            join.left,
            join.right,
            join.leftKeys,
            join.rightKeys,
            join.joinType,
            BuildLeft)
        } else if (rightSize < conf.autoBroadcastJoinThreshold) {
          BroadcastHashJoinExec(
            join.left,
            join.right,
            join.leftKeys,
            join.rightKeys,
            join.joinType,
            BuildRight)
        } else {
          join
        }
    }
  }
}
```

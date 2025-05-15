# Structured Streaming 组件

Structured Streaming是Spark的流处理引擎，它基于Spark SQL引擎构建，提供了一种高级API，使开发者能够像处理批量数据一样处理流数据。本章将详细介绍Structured Streaming的核心组件和实现机制。

## 1. StreamExecution架构

StreamExecution是Structured Streaming的核心执行引擎，负责管理流查询的生命周期和执行流程。

### 1.1 StreamExecution抽象类

StreamExecution定义在`org.apache.spark.sql.execution.streaming.StreamExecution`抽象类中：

```scala
abstract class StreamExecution(
    val sparkSession: SparkSession,
    val name: String,
    val checkpointRoot: String,
    val analyzedPlan: LogicalPlan,
    val sink: BaseStreamingSink,
    val trigger: Trigger,
    val triggerClock: Clock,
    val outputMode: OutputMode,
    val deleteCheckpointOnStop: Boolean)
  extends QueryExecutionBase(sparkSession, analyzedPlan) with Logging with Serializable {

  // 查询ID
  val id: UUID = UUID.randomUUID

  // 运行ID
  protected var runId: UUID = _

  // 当前偏移量
  protected var currentBatchId: Long = -1

  // 可用偏移量
  protected var availableOffsets: StreamProgress = new StreamProgress

  // 已提交偏移量
  protected var committedOffsets: StreamProgress = new StreamProgress

  // 水印
  protected var watermarkTracker: WatermarkTracker = _

  // 状态存储
  protected var stateStoreCoordinator: Option[StateStoreCoordinatorRef] = None

  // 查询状态
  @volatile protected var state: State = INITIALIZED

  // 异常
  @volatile protected var streamDeathCause: StreamingQueryException = null

  // 启动查询
  def start(): Unit

  // 停止查询
  def stop(): Unit

  // 等待终止
  def awaitTermination(): Unit

  // 执行批次
  protected def runBatch(sparkSessionToRunBatch: SparkSession): Unit

  // 获取偏移量
  protected def getOffsetSeqMetadata(
      sparkSessionToRunBatch: SparkSession,
      currentOffsets: OffsetSeq): OffsetSeqMetadata

  // 构建执行计划
  protected def runBatchWithNewSources(
      sparkSessionToRunBatch: SparkSession,
      newSources: Seq[BaseStreamingSource],
      metadataForNewSources: Option[OffsetSeqMetadata]): Unit

  // 其他方法...
}
```

### 1.2 StreamExecution的状态管理

StreamExecution使用状态机管理查询的生命周期：

```scala
// 状态定义
protected sealed trait State
protected object INITIALIZED extends State
protected object ACTIVE extends State
protected object TERMINATED extends State

// 状态转换
protected def transition(oldState: State, newState: State): Unit = synchronized {
  if (oldState == newState) {
    logWarning(s"Attempted to transition from $oldState to $newState, but current state is $state")
    return
  }

  // 检查转换是否有效
  (oldState, newState) match {
    case (INITIALIZED, ACTIVE) =>
      // 初始化到活动
      logInfo(s"Query $prettyIdString starting")

    case (ACTIVE, TERMINATED) =>
      // 活动到终止
      logInfo(s"Query $prettyIdString terminating")

    case (INITIALIZED, TERMINATED) =>
      // 初始化到终止
      logInfo(s"Query $prettyIdString terminating from INITIALIZED state")

    case _ =>
      // 无效转换
      throw new IllegalStateException(s"Invalid transition from $oldState to $newState")
  }

  // 更新状态
  state = newState

  // 触发状态变更监听器
  stateChangeListeners.foreach(_.onStateChange(oldState.toString, newState.toString))
}
```

### 1.3 StreamExecution的偏移量管理

StreamExecution使用StreamProgress管理各个源的偏移量：

```scala
// 流进度
class StreamProgress {
  // 源到偏移量的映射
  private val offsets = new ConcurrentHashMap[BaseStreamingSource, Offset]()

  // 获取偏移量
  def get(source: BaseStreamingSource): Option[Offset] = Option(offsets.get(source))

  // 设置偏移量
  def set(source: BaseStreamingSource, offset: Offset): Unit = {
    offsets.put(source, offset)
  }

  // 获取所有源
  def sources: Seq[BaseStreamingSource] = offsets.keySet().asScala.toSeq

  // 获取所有偏移量
  def toOffsetSeq(sources: Seq[BaseStreamingSource]): OffsetSeq = {
    OffsetSeq(sources.map(s => offsets.get(s)))
  }

  // 其他方法...
}
```

## 2. MicroBatchExecution实现

MicroBatchExecution是StreamExecution的一个具体实现，它使用微批处理模式执行流查询。

### 2.1 MicroBatchExecution的核心结构

MicroBatchExecution定义在`org.apache.spark.sql.execution.streaming.MicroBatchExecution`类中：

```scala
class MicroBatchExecution(
    sparkSession: SparkSession,
    name: String,
    checkpointRoot: String,
    analyzedPlan: LogicalPlan,
    sink: BaseStreamingSink,
    trigger: Trigger,
    triggerClock: Clock,
    outputMode: OutputMode,
    extraOptions: Map[String, String],
    deleteCheckpointOnStop: Boolean)
  extends StreamExecution(
    sparkSession, name, checkpointRoot, analyzedPlan, sink,
    trigger, triggerClock, outputMode, deleteCheckpointOnStop) {

  // 源
  private var sources: Seq[BaseStreamingSource] = _

  // 触发器执行器
  private var triggerExecutor: TriggerExecutor = _

  // 启动查询
  override def start(): Unit = {
    // 初始化状态
    state = INITIALIZED

    // 创建运行ID
    runId = UUID.randomUUID

    // 初始化水印跟踪器
    watermarkTracker = new WatermarkTracker(sparkSession.sessionState.conf)

    // 初始化状态存储协调器
    stateStoreCoordinator = Some(StateStoreCoordinatorRef.forDriver(
      sparkSession.sparkContext.env))

    // 获取源
    sources = logicalPlan.collect {
      case StreamingExecutionRelation(s, _) => s
      case StreamingRelationV2(_, _, _, s, _, _) => s
    }

    // 创建触发器执行器
    triggerExecutor = trigger match {
      case ProcessingTimeTrigger =>
        // 处理时间触发器
        new ProcessingTimeExecutor(processingTimeTriggerHandler)
      case OneTimeTrigger =>
        // 一次性触发器
        new OneTimeExecutor()
      case ContinuousTrigger(interval) =>
        // 连续触发器
        new ProcessingTimeExecutor(
          new FixedIntervalTriggerHandler(interval, triggerClock))
    }

    // 启动执行器
    triggerExecutor.execute(() => {
      startTrigger()

      // 如果查询仍然活动，执行批次
      if (isActive) {
        try {
          runBatch(sparkSession)
        } catch {
          case e: Throwable =>
            // 处理异常
            reportError("Error running batch", e)
        }
      }

      // 返回是否继续执行
      isActive
    })

    // 转换到活动状态
    transition(INITIALIZED, ACTIVE)
  }

  // 执行批次
  override protected def runBatch(sparkSessionToRunBatch: SparkSession): Unit = {
    // 记录批次开始时间
    val batchStartTime = triggerClock.getTimeMillis()

    // 获取可用偏移量
    val newData = getAvailableOffsets

    // 如果没有新数据，返回
    if (newData.isEmpty) {
      return
    }

    // 更新当前批次ID
    currentBatchId += 1

    // 更新水印
    updateWatermark(sparkSessionToRunBatch)

    // 构建执行计划
    val nextBatch = new Dataset(
      sparkSessionToRunBatch,
      logicalPlan,
      RowEncoder(logicalPlan.schema))

    // 执行查询
    reportTimeTaken("queryPlanning") {
      lastExecution = nextBatch.queryExecution
      lastExecution.executedPlan
    }

    // 写入接收器
    reportTimeTaken("addBatch") {
      sink.addBatch(currentBatchId, lastExecution)
    }

    // 提交偏移量
    committedOffsets ++= availableOffsets

    // 记录批次完成时间
    val batchDuration = triggerClock.getTimeMillis() - batchStartTime

    // 更新进度
    sparkSession.sparkContext.statusStore.putExecutorInfo(
      currentBatchId, batchDuration, lastExecution.toString)
  }

  // 其他方法...
}
```

### 2.2 MicroBatchExecution的偏移量获取

MicroBatchExecution通过`getAvailableOffsets`方法获取各个源的最新偏移量：

```scala
// 获取可用偏移量
private def getAvailableOffsets: Option[StreamProgress] = {
  // 创建新的进度对象
  val newProgress = new StreamProgress

  // 获取每个源的最新偏移量
  val hasNewData = sources.map { source =>
    // 获取当前偏移量
    val currentOffset = committedOffsets.get(source)

    // 获取最新偏移量
    val newOffset = source.getOffset

    // 如果有新偏移量，更新进度
    if (newOffset.isDefined && newOffset != currentOffset) {
      newProgress.set(source, newOffset.get)
      true
    } else {
      false
    }
  }.exists(identity)

  // 如果有新数据，返回进度
  if (hasNewData) Some(newProgress) else None
}
```

### 2.3 MicroBatchExecution的水印更新

MicroBatchExecution通过`updateWatermark`方法更新水印：

```scala
// 更新水印
private def updateWatermark(sparkSession: SparkSession): Unit = {
  // 获取水印操作符
  val watermarkOperators = lastExecution.executedPlan.collect {
    case e: EventTimeWatermarkExec => e
  }

  // 如果没有水印操作符，返回
  if (watermarkOperators.isEmpty) {
    return
  }

  // 更新水印
  watermarkOperators.foreach { w =>
    val newWatermark = w.eventTimeStats.value.max - w.delayMs

    // 如果新水印大于当前水印，更新
    if (newWatermark > watermarkTracker.currentWatermark) {
      logInfo(s"Updating event time watermark to: $newWatermark ms")
      watermarkTracker.updateWatermark(newWatermark)
    }
  }
}
```

## 3. ContinuousExecution实现

ContinuousExecution是StreamExecution的另一个具体实现，它使用连续处理模式执行流查询，提供更低的延迟。

### 3.1 ContinuousExecution的核心结构

ContinuousExecution定义在`org.apache.spark.sql.execution.streaming.continuous.ContinuousExecution`类中：

```scala
class ContinuousExecution(
    sparkSession: SparkSession,
    name: String,
    checkpointRoot: String,
    analyzedPlan: LogicalPlan,
    sink: BaseStreamingSink,
    trigger: ContinuousTrigger,
    triggerClock: Clock,
    outputMode: OutputMode,
    extraOptions: Map[String, String],
    deleteCheckpointOnStop: Boolean)
  extends StreamExecution(
    sparkSession, name, checkpointRoot, analyzedPlan, sink,
    trigger, triggerClock, outputMode, deleteCheckpointOnStop) {

  // 源
  private var sources: Seq[ContinuousReadSupport] = _

  // 查询执行线程
  private var queryExecutionThread: Thread = _

  // 启动查询
  override def start(): Unit = {
    // 初始化状态
    state = INITIALIZED

    // 创建运行ID
    runId = UUID.randomUUID

    // 初始化水印跟踪器
    watermarkTracker = new WatermarkTracker(sparkSession.sessionState.conf)

    // 初始化状态存储协调器
    stateStoreCoordinator = Some(StateStoreCoordinatorRef.forDriver(
      sparkSession.sparkContext.env))

    // 获取源
    sources = logicalPlan.collect {
      case r: StreamingDataSourceV2Relation if r.readSupport.isInstanceOf[ContinuousReadSupport] =>
        r.readSupport.asInstanceOf[ContinuousReadSupport]
    }

    // 检查源是否支持连续处理
    if (sources.isEmpty) {
      throw new IllegalStateException("No continuous sources found")
    }

    // 创建查询执行线程
    queryExecutionThread = new Thread("continuous-query-execution-thread") {
      override def run(): Unit = {
        // 执行查询
        runContinuous()
      }
    }

    // 启动线程
    queryExecutionThread.setDaemon(true)
    queryExecutionThread.start()

    // 转换到活动状态
    transition(INITIALIZED, ACTIVE)
  }

  // 执行连续查询
  private def runContinuous(): Unit = {
    // 获取初始偏移量
    val initialOffsets = getStartOffsets()

    // 创建连续读取计划
    val continuousReadPlan = ContinuousExecution.createReadPlan(
      logicalPlan, sources, initialOffsets)

    // 创建写入计划
    val writePlan = ContinuousWriteRDD.createWritePlan(
      continuousReadPlan, sink, outputMode)

    // 提交作业
    val job = sparkSession.sparkContext.submitJob(
      writePlan.rdd,
      (iter: Iterator[InternalRow]) => iter.foreach(_ => ()),
      0 until writePlan.rdd.getNumPartitions,
      (_, _) => (),
      ())

    // 等待作业完成
    job.awaitTermination()
  }

  // 执行批次（不适用于连续处理）
  override protected def runBatch(sparkSessionToRunBatch: SparkSession): Unit = {
    throw new IllegalStateException("ContinuousExecution does not support runBatch")
  }

  // 其他方法...
}
```

### 3.2 ContinuousExecution的偏移量管理

ContinuousExecution使用不同的偏移量管理机制：

```scala
// 获取起始偏移量
private def getStartOffsets(): OffsetSeq = {
  // 如果有已提交的偏移量，使用它们
  if (committedOffsets.nonEmpty) {
    committedOffsets.toOffsetSeq(sources.asInstanceOf[Seq[BaseStreamingSource]])
  } else {
    // 否则获取初始偏移量
    val offsets = sources.map { source =>
      source.getInitialOffset()
    }

    // 创建偏移量序列
    OffsetSeq(offsets)
  }
}
```

## 4. 状态存储

Structured Streaming使用状态存储来保存流查询的状态，支持有状态的操作如窗口聚合和流-流连接。

### 4.1 StateStore接口

StateStore定义在`org.apache.spark.sql.execution.streaming.state.StateStore`接口中：

```scala
trait StateStore {
  // 版本
  def version: Long

  // 获取键值对
  def get(key: UnsafeRow): UnsafeRow

  // 放入键值对
  def put(key: UnsafeRow, value: UnsafeRow): Unit

  // 移除键值对
  def remove(key: UnsafeRow): Unit

  // 获取所有键值对
  def getRange(start: Option[UnsafeRow], end: Option[UnsafeRow]): Iterator[(UnsafeRow, UnsafeRow)]

  // 提交更改
  def commit(): Long

  // 中止更改
  def abort(): Unit

  // 获取指标
  def metrics: StateStoreMetrics

  // 是否支持更新
  def hasCommitted: Boolean
}
```

### 4.2 HDFSBackedStateStore实现

HDFSBackedStateStore是StateStore的一个具体实现，它将状态存储在HDFS上：

```scala
class HDFSBackedStateStore(
    val id: StateStoreId,
    val keySchema: StructType,
    val valueSchema: StructType,
    val storeConf: StateStoreConf,
    val hadoopConf: Configuration,
    val version: Long,
    val sparkConf: SparkConf)
  extends StateStore {

  // 状态映射
  private val state = new ConcurrentHashMap[UnsafeRow, UnsafeRow]()

  // 已提交标志
  private var committed = false

  // 加载状态
  private def loadState(): Unit = {
    // 获取状态文件路径
    val stateFile = new Path(stateDir, s"$version.snapshot")

    // 如果文件存在，加载状态
    if (fs.exists(stateFile)) {
      val inputStream = fs.open(stateFile)
      try {
        // 读取键值对
        val keyValuePairs = StateStoreUtils.deserializeState(
          inputStream, keySchema, valueSchema)

        // 将键值对放入状态映射
        keyValuePairs.foreach { case (key, value) =>
          state.put(key, value)
        }
      } finally {
        inputStream.close()
      }
    }
  }

  // 获取键值对
  override def get(key: UnsafeRow): UnsafeRow = {
    state.get(key)
  }

  // 放入键值对
  override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
    // 复制键值对
    val keyCopy = key.copy()
    val valueCopy = value.copy()

    // 放入状态映射
    state.put(keyCopy, valueCopy)
  }

  // 移除键值对
  override def remove(key: UnsafeRow): Unit = {
    state.remove(key)
  }

  // 提交更改
  override def commit(): Long = {
    // 获取状态文件路径
    val stateFile = new Path(stateDir, s"${version + 1}.snapshot")

    // 创建输出流
    val outputStream = fs.create(stateFile)
    try {
      // 序列化状态
      StateStoreUtils.serializeState(
        state.entrySet().iterator().asScala.map(e => (e.getKey, e.getValue)),
        outputStream,
        keySchema,
        valueSchema)
    } finally {
      outputStream.close()
    }

    // 更新已提交标志
    committed = true

    // 返回新版本
    version + 1
  }

  // 其他方法...
}
```

## 5. 触发器

触发器控制流查询的执行频率，Structured Streaming提供了多种触发器实现。

### 5.1 Trigger接口

Trigger定义在`org.apache.spark.sql.streaming.Trigger`抽象类中：

```scala
abstract class Trigger

// 处理时间触发器
case object ProcessingTimeTrigger extends Trigger

// 一次性触发器
case object OneTimeTrigger extends Trigger

// 连续触发器
case class ContinuousTrigger(intervalMs: Long) extends Trigger
```

### 5.2 TriggerExecutor实现

TriggerExecutor负责执行触发器，定义在`org.apache.spark.sql.execution.streaming.TriggerExecutor`接口中：

```scala
trait TriggerExecutor {
  // 执行触发器
  def execute(callback: () => Boolean): Unit
}

// 处理时间执行器
class ProcessingTimeExecutor(triggerHandler: TriggerHandler) extends TriggerExecutor {
  // 执行触发器
  override def execute(callback: () => Boolean): Unit = {
    // 循环执行
    while (true) {
      // 等待下一次触发
      triggerHandler.waitForTrigger()

      // 执行回调
      if (!callback()) {
        // 如果回调返回false，停止执行
        return
      }
    }
  }
}

// 一次性执行器
class OneTimeExecutor extends TriggerExecutor {
  // 执行触发器
  override def execute(callback: () => Boolean): Unit = {
    // 执行一次回调
    callback()
  }
}
```

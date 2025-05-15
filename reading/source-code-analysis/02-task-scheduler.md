# 任务调度器

Spark的任务调度系统是其核心组件之一，负责将逻辑执行计划转换为物理任务，并在集群上高效地调度和执行这些任务。Spark的调度系统主要由DAGScheduler和TaskScheduler两个组件组成，它们共同协作完成任务的调度和执行。

## 1. DAGScheduler实现

DAGScheduler是Spark调度系统的高层调度器，负责将Spark作业转换为Stage的DAG（有向无环图），并跟踪哪些RDD和Stage输出已经被物化。它决定每个任务的最佳执行位置，并将Stage以TaskSet的形式传递给底层的TaskScheduler。

### 1.1 DAGScheduler的核心数据结构

DAGScheduler定义在`org.apache.spark.scheduler.DAGScheduler`类中：

```scala
private[spark] class DAGScheduler(
    private[scheduler] val sc: SparkContext,
    private[scheduler] val taskScheduler: TaskScheduler,
    listenerBus: LiveListenerBus,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv,
    clock: Clock = new SystemClock())
  extends Logging {

  // Stage ID生成器
  private[scheduler] val nextStageId = new AtomicInteger(0)

  // Job ID生成器
  private[scheduler] val nextJobId = new AtomicInteger(0)

  // 等待中的作业
  private[scheduler] val waitingStages = new HashSet[Stage]

  // 运行中的作业
  private[scheduler] val runningStages = new HashSet[Stage]

  // 失败的Stage
  private[scheduler] val failedStages = new HashSet[Stage]

  // Job到Stage的映射
  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]

  // Stage到Job的映射
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]

  // 其他数据结构...

  // 事件处理循环
  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)

  // 其他方法...
}
```

### 1.2 Stage的划分

DAGScheduler将Spark作业划分为多个Stage，Stage的边界由Shuffle操作决定：

```scala
private[scheduler] def getParentStagesAndId(
    rdd: RDD[_],
    firstJobId: Int): (List[Stage], Int) = {
  // 获取或创建Stage
  val parentStages = getParentStages(rdd, firstJobId)
  val id = nextStageId.getAndIncrement()
  (parentStages, id)
}

private[scheduler] def getParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
  val parents = new HashSet[Stage]
  val visited = new HashSet[RDD[_]]

  // 递归查找父Stage
  def visit(r: RDD[_]): Unit = {
    if (!visited(r)) {
      visited += r

      // 对于ShuffleDependency，创建新的Stage
      for (dep <- r.dependencies) {
        dep match {
          case shufDep: ShuffleDependency[_, _, _] =>
            parents += getOrCreateShuffleMapStage(shufDep, firstJobId)
          case _ =>
            visit(dep.rdd)
        }
      }
    }
  }

  visit(rdd)
  parents.toList
}
```

### 1.3 作业提交

DAGScheduler通过`submitJob`方法提交作业：

```scala
def submitJob[T, U](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    callSite: CallSite,
    resultHandler: (Int, U) => Unit,
    properties: Properties): JobWaiter[U] = {
  // 创建JobWaiter
  val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)

  // 发送JobSubmitted事件
  eventProcessLoop.post(
    JobSubmitted(
      jobId, rdd, func, partitions, callSite, waiter,
      SerializationUtils.clone(properties)))

  waiter
}
```

当DAGScheduler接收到JobSubmitted事件时，它会创建Stage并提交：

```scala
private[scheduler] def handleJobSubmitted(jobId: Int,
    finalRDD: RDD[_],
    func: (TaskContext, Iterator[_]) => _,
    partitions: Array[Int],
    callSite: CallSite,
    listener: JobListener,
    properties: Properties): Unit = {

  // 创建最终Stage（ResultStage）
  val finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)

  // 创建ActiveJob
  val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)

  // 记录Job信息
  clearCacheLocs()
  jobIdToActiveJob(jobId) = job
  activeJobs += job
  finalStage.setActiveJob(job)

  // 将Stage添加到等待队列
  submitStage(finalStage)

  // 提交等待中的Stage
  submitWaitingStages()
}
```

### 1.4 Stage提交

DAGScheduler通过`submitStage`方法提交Stage：

```scala
private def submitStage(stage: Stage): Unit = {
  val jobId = activeJobForStage(stage)
  if (jobId.isDefined) {
    logDebug(s"submitStage($stage)")

    // 检查Stage是否已经提交
    if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
      // 获取父Stage
      val missing = getMissingParentStages(stage).sortBy(_.id)

      // 如果有缺失的父Stage，先提交父Stage
      if (missing.isEmpty) {
        logInfo(s"Submitting $stage (${stage.rdd}), which has no missing parents")
        submitMissingTasks(stage, jobId.get)
        runningStages += stage
      } else {
        // 递归提交父Stage
        for (parent <- missing) {
          submitStage(parent)
        }
        waitingStages += stage
      }
    }
  } else {
    abortStage(stage, "No active job for stage " + stage.id, None)
  }
}
```

### 1.5 任务提交

当Stage的所有父Stage都完成后，DAGScheduler会提交该Stage的任务：

```scala
private def submitMissingTasks(stage: Stage, jobId: Int): Unit = {
  // 获取Stage的分区位置
  val taskIdToLocations = getPreferredLocs(stage.rdd, 0 until stage.numPartitions)

  // 创建任务
  val tasks: Seq[Task[_]] = stage match {
    case stage: ShuffleMapStage =>
      partitionsToCompute.map { id =>
        val locs = taskIdToLocations(id)
        val part = stage.rdd.partitions(id)
        new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
          taskBinary, part, locs, properties, serializedTaskMetrics, Option(jobId),
          Option(sc.applicationId), sc.applicationAttemptId, stage.rdd.isBarrier())
      }

    case stage: ResultStage =>
      partitionsToCompute.map { id =>
        val p = stage.partitions(id)
        val part = stage.rdd.partitions(p)
        val locs = taskIdToLocations(id)
        new ResultTask(stage.id, stage.latestInfo.attemptId,
          taskBinary, part, locs, id, properties, serializedTaskMetrics,
          Option(jobId), Option(sc.applicationId), sc.applicationAttemptId,
          stage.rdd.isBarrier())
      }
  }

  // 提交任务集
  stage.pendingPartitions ++= tasks.map(_.partitionId)
  taskScheduler.submitTasks(new TaskSet(
    tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
}
```

## 2. TaskScheduler实现

TaskScheduler是Spark调度系统的底层调度器，负责将DAGScheduler提交的TaskSet分配给集群中的执行器，并处理任务的执行结果。TaskScheduler通过SchedulerBackend与集群管理器（如YARN、Mesos或Standalone）交互。

### 2.1 TaskScheduler的核心数据结构

TaskScheduler定义在`org.apache.spark.scheduler.TaskSchedulerImpl`类中：

```scala
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false)
  extends TaskScheduler with Logging {

  // 调度器后端
  var backend: SchedulerBackend = null

  // 调度池
  val rootPool: Pool = new Pool("", schedulingMode, 0, 0)

  // 任务集管理器
  val taskSetManagers = new HashMap[String, TaskSetManager]

  // 黑名单
  private val blacklistTrackerOpt = if (BlacklistTracker.isBlacklistEnabled(conf)) {
    Some(new BlacklistTracker(conf, listenerBus, clock))
  } else {
    None
  }

  // 其他数据结构...

  // 其他方法...
}
```

### 2.2 任务提交

TaskScheduler通过`submitTasks`方法接收DAGScheduler提交的TaskSet：

```scala
override def submitTasks(taskSet: TaskSet): Unit = {
  val tasks = taskSet.tasks
  logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")

  this.synchronized {
    // 创建TaskSetManager
    val manager = createTaskSetManager(taskSet, maxTaskFailures)
    val stage = taskSet.stageId
    val stageTaskSets = taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
    stageTaskSets(taskSet.stageAttemptId) = manager

    // 检查是否有冲突的Stage
    val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
      ts.taskSet != taskSet && !ts.isZombie
    }

    if (conflictingTaskSet) {
      // 处理冲突...
    } else {
      // 将TaskSetManager添加到调度池
      taskSetManagers(taskSet.id) = manager
      rootPool.addSchedulable(manager)

      // 如果后端已经启动，则开始调度
      if (backend.isReady()) {
        backend.reviveOffers()
      }
    }
  }
}
```

### 2.3 任务调度

TaskScheduler通过`resourceOffers`方法将资源分配给任务：

```scala
def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
  // 过滤黑名单中的执行器
  val filteredOffers = blacklistTrackerOpt.map { blacklistTracker =>
    offers.filter { offer =>
      !blacklistTracker.isNodeBlacklisted(offer.host) &&
      !blacklistTracker.isExecutorBlacklisted(offer.executorId)
    }
  }.getOrElse(offers)

  // 打乱执行器顺序，避免总是使用同一个执行器
  val shuffledOffers = shuffleOffers(filteredOffers)

  // 构建主机到执行器的映射
  val hostToExecutors = new HashMap[String, Set[String]]

  // 构建主机到机架的映射
  val hostToRacks = new HashMap[String, String]

  // 初始化映射...

  // 分配任务
  val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores / CPUS_PER_TASK))
  val availableCpus = shuffledOffers.map(o => o.cores).toArray

  // 按照调度池分配任务
  for (taskSet <- rootPool.getSortedTaskSetQueue) {
    // 为每个TaskSet分配资源
    taskSet.assignTasks(availableCpus, shuffledOffers, availableSlots, tasks)
  }

  // 返回任务分配结果
  tasks
}
```

### 2.4 任务执行结果处理

TaskScheduler通过`statusUpdate`方法处理任务执行结果：

```scala
def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer): Unit = {
  // 查找任务
  val taskDesc = taskIdToTaskSetManager.get(tid)

  if (taskDesc.isDefined) {
    // 获取TaskSetManager
    val manager = taskDesc.get._1

    // 处理任务状态更新
    manager.statusUpdate(taskDesc.get._2, state, serializedData)

    // 如果任务完成，则尝试调度新任务
    if (state == TaskState.FINISHED) {
      backend.reviveOffers()
    }
  } else {
    // 处理未知任务...
  }
}
```

## 3. SchedulerBackend实现

SchedulerBackend是TaskScheduler与集群管理器之间的接口，负责与集群管理器交互，获取资源并启动执行器。Spark提供了多种SchedulerBackend实现，以支持不同的集群管理器。

### 3.1 SchedulerBackend接口

SchedulerBackend定义在`org.apache.spark.scheduler.SchedulerBackend`接口中：

```scala
private[spark] trait SchedulerBackend {
  // 启动后端
  def start(): Unit

  // 停止后端
  def stop(): Unit

  // 唤醒后端，请求资源
  def reviveOffers(): Unit

  // 杀死任务
  def killTask(taskId: Long, executorId: String, interruptThread: Boolean, reason: String): Unit

  // 获取默认级别
  def defaultParallelism(): Int

  // 后端是否准备好
  def isReady(): Boolean = true

  // 应用ID
  def applicationId(): String = ""

  // 应用尝试ID
  def applicationAttemptId(): Option[String] = None

  // 获取执行器丢失原因
  def getExecutorLossReason(executorId: String): Option[ExecutorLossReason] = None
}
```

### 3.2 CoarseGrainedSchedulerBackend

CoarseGrainedSchedulerBackend是一种常用的SchedulerBackend实现，它使用粗粒度的资源分配模式，一次分配一个执行器，而不是一次分配一个任务。这种模式适用于YARN、Mesos和Standalone集群管理器。

```scala
private[spark] abstract class CoarseGrainedSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    val rpcEnv: RpcEnv)
  extends ExecutorAllocationClient with SchedulerBackend with Logging {

  // 执行器数据
  protected val executorDataMap = new ConcurrentHashMap[String, ExecutorData]

  // 总的核心数
  protected var totalCoreCount = new AtomicInteger(0)

  // 最大核心数
  protected var totalRegisteredExecutors = new AtomicInteger(0)

  // 驱动器端点
  private val driverEndpoint = rpcEnv.setupEndpoint(
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME, createDriverEndpoint())

  // 创建驱动器端点
  protected def createDriverEndpoint(): CoarseGrainedSchedulerBackend.DriverEndpoint

  // 启动后端
  override def start(): Unit = {
    // 实现...
  }

  // 停止后端
  override def stop(): Unit = {
    // 实现...
  }

  // 唤醒后端
  override def reviveOffers(): Unit = {
    driverEndpoint.send(ReviveOffers)
  }

  // 杀死任务
  override def killTask(
      taskId: Long,
      executorId: String,
      interruptThread: Boolean,
      reason: String): Unit = {
    driverEndpoint.send(KillTask(taskId, executorId, interruptThread, reason))
  }

  // 其他方法...
}
```

### 3.3 DriverEndpoint

DriverEndpoint是CoarseGrainedSchedulerBackend中的一个关键组件，它处理与执行器的通信：

```scala
protected class DriverEndpoint extends ThreadSafeRpcEndpoint {
  // 处理执行器注册
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterExecutor(executorId, executorRef, hostname, cores, logUrls) =>
      // 注册执行器
      if (executorDataMap.containsKey(executorId)) {
        // 执行器已存在，拒绝注册
        context.reply(RegisterExecutorFailed("Duplicate executor ID: " + executorId))
      } else {
        // 创建执行器数据
        val data = new ExecutorData(executorRef, hostname, cores, logUrls)
        executorDataMap.put(executorId, data)
        totalCoreCount.addAndGet(cores)
        totalRegisteredExecutors.incrementAndGet()

        // 回复注册成功
        context.reply(RegisteredExecutor)

        // 唤醒调度器
        reviveOffers()
      }

    // 其他消息处理...
  }

  // 处理资源提供
  override def receive: PartialFunction[Any, Unit] = {
    case ReviveOffers =>
      // 创建资源提供
      val offers = executorDataMap.asScala.map { case (id, data) =>
        new WorkerOffer(id, data.executorHost, data.freeCores)
      }.toIndexedSeq

      // 分配任务
      val tasks = scheduler.resourceOffers(offers)

      // 将任务发送给执行器
      for (i <- 0 until tasks.length) {
        val executorId = offers(i).executorId
        val executorData = executorDataMap.get(executorId)

        for (task <- tasks(i)) {
          executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(task.serializedTask)))
        }
      }

    // 其他消息处理...
  }
}
```

### 3.4 执行器管理

CoarseGrainedSchedulerBackend负责管理执行器的生命周期：

```scala
// 添加执行器
def addExecutors(executorIds: Seq[String]): Unit = {
  executorIds.foreach { id =>
    executorDataMap.put(id, new ExecutorData(
      null, null, 0, Map.empty, ExecutorState.LOADING))
  }
}

// 移除执行器
def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
  // 从执行器数据映射中移除
  val executorData = executorDataMap.remove(executorId)

  if (executorData != null) {
    // 更新核心数
    totalCoreCount.addAndGet(-executorData.totalCores)
    totalRegisteredExecutors.decrementAndGet()

    // 通知TaskScheduler
    scheduler.executorLost(executorId, reason)

    // 通知执行器分配管理器
    listenerBus.post(SparkListenerExecutorRemoved(
      System.currentTimeMillis(), executorId, reason.toString))
  }
}
```

## 4. 任务调度流程

Spark的任务调度流程涉及多个组件的协作，下面是一个完整的任务调度流程：

1. **作业提交**：
   - 用户调用`SparkContext.runJob`提交作业
   - `SparkContext`将作业转发给`DAGScheduler`

2. **Stage划分**：
   - `DAGScheduler`根据RDD的依赖关系将作业划分为多个Stage
   - 在ShuffleDependency处划分Stage边界

3. **Stage提交**：
   - `DAGScheduler`按照拓扑顺序提交Stage
   - 对于每个Stage，先提交其所有父Stage

4. **任务创建**：
   - 对于每个Stage，`DAGScheduler`为每个分区创建一个任务
   - 根据Stage类型创建ShuffleMapTask或ResultTask

5. **任务提交**：
   - `DAGScheduler`将任务打包为TaskSet提交给`TaskScheduler`
   - `TaskScheduler`创建TaskSetManager管理TaskSet

6. **资源分配**：
   - `SchedulerBackend`从集群管理器获取资源
   - `TaskScheduler`根据资源和任务的本地性需求分配任务

7. **任务执行**：
   - `SchedulerBackend`将任务发送给执行器
   - 执行器执行任务并返回结果

8. **结果处理**：
   - `TaskScheduler`接收任务执行结果
   - `DAGScheduler`处理Stage完成事件，触发下一个Stage的提交
   - 当最终Stage完成时，作业完成

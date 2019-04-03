---
typora-copy-images-to: 图片
---

# citus连接的创建与复用源码浅析（刘攀）

本文主要结合citus8.1.1的源码，来讲解citus执行SQL的大体流程以及新增的四种执行器和五种执行方法，同时也讲解citus的CN和worker之间要建立连接时，如何决定创建长连接还是短连接的，以及这两种连接分别是如何创建的；并且通过分析几种场景，来说明目前citus在单一事务中涉及多个分片操作的DML语句时存在的一些限制，以及讲解citus自带的连接复用功能。

<h2 id="1">一、citus分布式查询规划和处理</h3>

### 1.1、查询处理流程

Citus群集是由一个coordinator 和多个worker组成的。在workers上存储实际的分片数据，而coordinator上存储有关这些分片的元数据。所有发送到集群的查询都是先通过coordinator 执行，coordinator将查询分解为较小的查询片段，其中每个查询片段可以在分片上独立运行，然后coordinator将查询片段分配给workers，合并workers返回的结果，并将最终结果返回给用户。查询处理体系架构可以通过下面官网上的图来简要描述：

![citus-high-level-arch.png](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/citus-high-level-arch.png?raw=true)

<center>图1、citus查询处理流程<center\>

从上可以看出，citus查询处理通道涉及两个组件

- **Citus Distributed Query Planner（分布式查询规划器） and Executor（执行器）**
- **PostgreSQL Query  Planner（查询规划器） and Executor**

因为本文只重点关注Citus中和连接相关的内容，而citus分布式查询规划器和执行器，直接影响执行方法的选择，进而影响后续连接复用和新建的流程，所以此处简单分别介绍下这两个：

#### 1.1.1 分布式查询规划器（Distributed Query Planner）

Citus的分布式查询规划器是通过pg的“钩子”功能实现的，在_PG_init函数中，会把其distributed_planner()函数赋值给planner_hook，以便在pg查询规划阶段调用planner()函数时，直接调用planner_hook所指向函数。

Citus的分布式查询规划器，会接收一个SQL查询，并为其规划分布式执行方式，而对于SELECT操作和修改操作，生成执行计划的过程略有不同的，下面简单说明下（详细内容由其他同事分享）：

对于SELECT查询（不带分布键），citus的查询规划器将按照下面三步来进行：

- 首先，会对输入的查询生成一个计划树，并将其转换为一种便于交换和关联的形式，以便可以并行化操作。它还应用了多种优化，以确保以可伸缩的方式执行查询，并最大限度地减少网络I/O.

- 其次，规划器会把计划树，拆解为两部分执行计划： 在coordinator 上运行的查询计划和在worker上的各个分片上运行的查询片段，并且对于后者的那些查询片段，会分别分配给指定的worker，以便有效地使用它们的所有资源。
- 最后，将生成的分布式查询计划，传递给对应的分布式执行器以供执行。

对于修改操作或者指定了分布键的查找操作，其执行计划生成过程略有不同，因为它们只针对特定的分片。一旦规划器收到传入的查询后，会将该查询路由到其对应的正确分片上，具体做法是：

- 首先，它将从传入行数据中提取出分布列值，并查找元数据以确定涉及的正确分片。

- 然后，规划器重写该命令的SQL，以引用分片表而不是原始表。

- 最后，将此重写的执行计划传递给分布式执行程序。

其实对于不带分布键的SELECT，也会涉及到查找元数据，但是它会查找出该表对应的所有分片，并且根据这些分片制定对应worker的执行计划。

#### 1.1.2 分布式查询执行器（**Distributed Query Executor**）

对于Postgres的“查询执行”而言，该流程包括：

- 1、Portal（策略选择模块）根据输入的执行计划从五种执行策略（PORTAL_ONE_SELECT,	PORTAL_ONE_RETURNING,PORTAL_ONE_MOD_WITH,PORTAL_UTIL_SELECT,	PORTAL_MULTI_QUERY）中选择一种，并且据此选择相应的执行部件；

  > 1）PORTAL_ONE_SELECT：处理单个的SELECT语句，调用Executor模块；
  >
  > 2）PORTAL_ONE_RETURNING：处理带RETURNING的UPDATE/DELETE/INSERT语句，调用Executor模块（eg:insert into tbl1(id,name) values(6,'11') returning id;）；
  >
  > 3）PORTAL_UTIL_SELECT：处理单个的数据定义语句，调用ProcessUtility模块；
  >
  > 4）PORTAL_ONE_MOD_WITH：处理带有INSERT/UPDATE/DELETE的WITH子句的SELECT，其处理逻辑类似PORTAL_ONE_RETURNING。调用Executor模块；
  >
  > 5）PORTAL_MULTI_QUERY：是前面几种策略的混合，可以处理多个原子操作。

- 2、将执行流程交给相应的处理部件（ProcessUtility和Executor）；
- 3、若是对数据表中的元组进行增删改查操作，则会选择Executor部件进行后续的执行处理。

具体的执行流程，如下图所示：

![1552468672073](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552468672073.png?raw=true)

<center>图2、postgres查询执行流程<center\>

而Citus分布式查询执行器是用于运行分布式查询计划，并处理查询执行期间发生的异常，它是借助pg自带的CustomScan功能实现的，通过在_PG_init函数中调用RegisterCitusCustomScanMethods函数，把其自行实现的四种执行器注册到custom_scan_methods哈希表中 。

Citus注册的四种基本的执行器为：real-time、 router、 task tracker和COORDINATOR_INSERT_SELECT（官网中称只有前面三种，源码中四种）。它根据每个查询的结构，动态的选择（JobExecutorType函数）使用其中的一个或多个执行器；为了尽可能支持SQL功能，它会根据需求为不同的子查询或CTE分配不同的执行器，并且这个过程是递归的：如果Citus无法确定如何运行当前子查询，那么它会检查其子子查询。

下面简单介绍下这四种执行器（详细执行流程由组内其他人员介绍，也可以通过EXPLAIN操作查看某条SQL涉及到哪些执行器），其中四种执行器在源码中的类型定义如下：

```c
/* Enumeration that represents distributed executor types */
typedef enum
{
	MULTI_EXECUTOR_INVALID_FIRST = 0,
	MULTI_EXECUTOR_REAL_TIME = 1,
	MULTI_EXECUTOR_TASK_TRACKER = 2,
	MULTI_EXECUTOR_ROUTER = 3,
	MULTI_EXECUTOR_COORDINATOR_INSERT_SELECT = 4
} MultiExecutorType;
```

### 1.2、四种基本执行器

#### 1.2.1、real-time执行器

real-time执行器是Citus使用的默认执行程序，它非常适合快速响应涉及过滤（filters）、聚合、亲和表间的join操作；real-time执行器为每个涉及到的分片创建一个与其所在worker的连接，并将所有对应的执行计划发送到那些连接上，然后，它从这些连接中获取对应分片的响应结果，合并它们，并将最终结果返回给用户。

如果执行器无法将执行任务发送给指定的worker，或者任务执行失败，则执行器会动态地将该执行任务分配给其他worker上的对应副本。执行器在处理错误异常时仅处理失败的查询子树，而不会处理整个查询流程。

real-time执行器对应查询操作，会为每个待查询的分片（在生成计划期间会将不相关的分片剔除掉），都将打开一个连接，并使用两个文件描述符。因此，如果查询遇到大量分片，执行程序可能需要打开比max_connections更多的连接，或者使用比max_files_per_process更多的文件描述符。

适合场景：

1、不带分布键的查询操作（只涉及复制表则是调用router执行器）。

```sql
--tbl1和tbl2是分布表且是亲和表，tbl3是复制表
select * from tbl2 where name = '1';
```

2、过滤（filters）、聚合、亲和表间的join、分布表和复制表间的join操作。

```sql
--tbl1和tbl2是分布表且是亲和表，tbl3是复制表
select  from tbl2 a join tbl1 b on a.id=b.id;
select  from tbl3 a join tbl1 b on a.id=b.id;
select count(*) from tbl2;
--对于下面的SQL，会先采用real-time执行器获取子查询select的结果，然后采用router执行器在对应分片执行更新
update tbl2 set name = '3' where name in (select b.name from tbl1 a, tbl2 b where  a.id=b.id);
```

#### 1.2.2、router执行器

router执行器适合于修改操作或带分布键的查询操作，即能够明确知道该操作涉及到哪些具体分片上，它会为每个涉及到的分片创建一个与其所在worker的连接，并将所有对应的执行计划发送到那些连接上，然后它从这些连接中获取对应分片的响应结果，合并它们，并将最终结果返回给用户。

对于修改操作，如果router执行器无法将执行任务发送给指定的worker，或者任务执行失败，则执行器会回滚整个事务。

和real-time执行器类似，router执行器也会为涉及的每个分片都将打开一个连接，并使用两个文件描述符，同样存在超过连续限制和文件描述符限制。

适合场景：

1、带分布键的查询操作

2、对复制表的查询操作

3、修改操作

#### 1.2.3、task tracker执行器

task tracker执行器非常适合耗时较长的复杂的数据仓库查询，此执行器仅为每个worker创建一个连接，并将所有涉及到该work的查询计划，发送给该worker上的任务跟踪守护进程让其执行。然后，worker上的任务跟踪守护进程会定期安排新的执行任务并查看完成情况。coordinator上的task tracker执行器会定期检查这些任务跟踪守护进程，看看他们所负责的任务是否完成；若所有work上的任务都完成，则会收集所有结果，合并后发给客户端。

注意：因为该执行器对任何SQL操作获取连接的个数是一定的，所以本文不对其进行分析，具体的执行流程由其他同事进行分享。

```sql
--在会话中运行
set citus.task_exextor_type='task-tracker';
```

#### 1.2.4、cn_insert_select执行器

COORDINATOR_INSERT_SELECT（简称CN_INSERT_SELECT）执行器，专门用于coordinate处理某些INSERT … SELECT操作，它会先从worker节点中查询数据，将数据拉到coordinate节点，然后从coordinate发出带有数据的INSERT语句；最常见的步骤是先根据SELECT获取所有待插入的数据，然后通过COPY的方式，把数据发送到对应的分片上。

对于INSERT … SELECT操作有几种情况并不会走CN_INSERT_SELECT执行器：

情况1：如果SELECT操作带了分片键，那么会调用Router执行器。

情况2：如果源表和目标表是亲和表时，则直接采用Router执行器，将修改后的执行语句下推送到所有worker节点上并行执行。

适合场景：

1、非亲和关系的表之间进行数据导入导出

```sql
 --tbl1和tbl2是分布表且是亲和表，tbl3是复制表，tbl4是分布表，但是不是tbl1的亲和表，tbl5是本地表
 --1、导入导出的两个表是亲和表，则直接采用Router执行器，直接把执行计划下推
 insert into tbl2 select * from tbl1 ;
 --2、导出的表是复制表，则CN_INSERT_SELECT执行器会先用Router执行器查询出复制表所有数据，然后采用COPY方式把这些数据，分别拷贝到分布表对应分片上。
 insert into tbl2 select * from tbl3 ;
 --3、导入导出的两个表是非亲和表，则CN_INSERT_SELECT执行器会先用Real-Time执行器查询出导出表所有数据，然后采用COPY方式把这些数据，分别拷贝到分布表对应分片上。
 insert into tbl2 select * from tbl4 ;
 --4、导出的表是本地表，则CN_INSERT_SELECT执行器会直接先读取本地表中所有数据，然后采用COPY方式把这些数据，分别拷贝到分布表对应分片上。
 insert into tbl2 select * from tbl5 ;
```

2、采用generate_series等方式生成数据进行导入

```sql
--CN先生成待导入数据，然后CN_INSERT_SELECT执行器会采用COPY方式把这些数据，分别拷贝到分布表对应分片上。
insert into tbl2 select generate_series(1,20),12346;
```

### 1.3、五种任务执行方法

citus内部，四种不同的执行器，最终会根据具体的执行计划，寻找出一种任务执行方法，负责执行任务的具体执行；这是借助postgres自带的CustomScan APIs来实现，主要实现流程如下所示：

![1552465191872](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552465191872.png?raw=true)

<center>图3、citus任务执行流程<center\>

上图是一个Router执行器对于修改操作任务执行方法的注册和调用流程，而在源码中采用同样的方式，一共定义和注册了5种任务执行方法：

```c
/*
 * Define executor methods for the different executor types.
 */
static CustomExecMethods RealTimeCustomExecMethods = {
	.CustomName = "RealTimeScan", 
	.BeginCustomScan = CitusSelectBeginScan,
	.ExecCustomScan = RealTimeExecScan,//Real Time执行器的任务处理函数
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan//处理explain操作
};

static CustomExecMethods TaskTrackerCustomExecMethods = {
	.CustomName = "TaskTrackerScan",
	.BeginCustomScan = CitusSelectBeginScan,
	.ExecCustomScan = TaskTrackerExecScan, //Task Tracker执行器的任务处理函数
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan//处理explain操作
};

static CustomExecMethods RouterModifyCustomExecMethods = {
	.CustomName = "RouterModifyScan",
	.BeginCustomScan = CitusModifyBeginScan,
	.ExecCustomScan = RouterModifyExecScan, //Router执行器中对于修改操作的任务处理函数
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan//处理explain操作
};

static CustomExecMethods RouterSelectCustomExecMethods = {
	.CustomName = "RouterSelectScan",
	.BeginCustomScan = CitusSelectBeginScan,
	.ExecCustomScan = RouterSelectExecScan,//Router执行器中对于查询操作的任务处理函数
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan//处理explain操作
};

static CustomExecMethods CoordinatorInsertSelectCustomExecMethods = {
	.CustomName = "CoordinatorInsertSelectScan",
	.BeginCustomScan = CitusSelectBeginScan,
	.ExecCustomScan = CoordinatorInsertSelectExecScan,//CN上处理INSERT ... SELECT的特殊处理
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CoordinatorInsertSelectExplainScan//处理explain操作
};

```

其实从上面五种执行方法的函数命名就可以看出，除了Router执行器，根据操作类型是查询还是修改，分别定义了一种执行方法（在RouterCreateScan函数中进行选主）外，其他三种执行器各自定义了一种执行方法。

因为常用的操作，只涉及到Real Time执行器和Router执行器，所以接下来就详细讲述下这两种执行器涉及的三种执行方法中，和连接相关的代码实现流程，也就是以下三种：

- RealTimeExecScan函数：Real Time执行器的任务执行方法
- RouterModifyExecScan函数：Router执行器中对于修改操作的任务执行方法
- RouterSelectExecScan函数：Router执行器中对于查询操作的任务执行方法

上面三种任务执行方法，虽然执行流程不同，但是最终给每个任务获取和worker进行通信的连接方式是一样的，具体和连接相关的执行流程如下所示：

![1552485750203](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552485750203.png?raw=true)

<center>图4、citus任务执行方法选择连接的流程图<center\>

### 1.4、Router执行器之DML操作

目前观察到的，对于修改操作（INSERT、UPDATE或DELETE）的任务执行方法都是由Router执行器的RouterModifyExecScan函数来完成，该函数的代码实现如下所示：

```c
/*
 * RouterModifyExecScan executes a list of tasks on remote nodes, retrieves
 * the results and, if RETURNING is used or SELECT FOR UPDATE executed,
 * returns the results with a TupleTableSlot.
 *
 * The function can handle both single task query executions,
 * sequential or parallel multi-task query executions.
 */
TupleTableSlot *RouterModifyExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	。。。。。。
	if (!scanState->finishedRemoteScan)
	{
		。。。。。。
		bool parallelExecution = true;
		ExecuteSubPlans(distributedPlan);//执行子计划

		if (list_length(taskList) <= 1 ||  //该执行计划中的任务个数最多一个
			IsMultiRowInsert(workerJob->jobQuery) || //给定查询是INSERT且有RTE_VALUES
			MultiShardConnectionType == SEQUENTIAL_CONNECTION) //配置的连接类型是否为顺序连接
		{
			parallelExecution = false;//不进行并行操作
		}

		if (parallelExecution)
		{
			RouterMultiModifyExecScan(node);//并行执行修改操作
		}
		else
		{
			RouterSequentialModifyExecScan(node);//顺序（串行）执行修改操作
		}

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReturnTupleFromTuplestore(scanState);
	return resultSlot;
}
```

从上面的代码中，也可以看到，citus对于修改操作的执行，根据任务个数以及配置参数分为串行和并行两种执行方式，其中函数RouterMultiModifyExecScan负责并行处理修改操作，而函数RouterSequentialModifyExecScan处理串行修改操作；

从性能上来讲，并行执行明显优于串行，它会先把每个分片对应的执行计划同时发送到该分片所在worker上，然后在统一接收每个分片返回的结果；而不是像串行那样，发送一个执行计划接收到其结果后，才会发送另一个分片的执行计划；只有不满足下面三个条件时，才能够选择并行执行方式：

- 条件1、该执行计划中的任务个数最多一个，也就是涉及到的分片最多一个

- 条件2、该执行计划是执行INSERT操作（注意INSERT...SELECT是进入另一个执行器，所以此处是INSERT...VALUES），并且是同时插入多行值（判断条件为是否存在RTE_VALUES字段）

- 条件3、配置参数citus.multi_shard_modify_mode为sequential，而不是parallel（默认值），因为该值决定MultiShardConnectionType的取值。

对于条件2作用，还有待进一步分析，比如对于如下SQL：

```sql
--tbl2为分布表
insert into tbl2(id,name) values(6,'11'),(7,'12'); 
```

因为是同时插入两条数据，而且这两条数据位于不同的分片上，按理说是可以采用并行执行方式的，但是，虽然它执行计划中任务个数为2个，不满足条件1，但是由于插入多行，所以满足条件2，使得只能够采取串行方式。

下面两个小节，将分别介绍下修改操作的串行执行方式和并行执行方式：

#### 1.4.1、串行任务执行（修改操作）

当执行的DML（insert、update、delete），带了分片字段时，如执行以下类型的语句：

```sql
--tbl2为分布表
insert into tbl2(id,name) values(6,'11'),(7,'12'); 
insert into tbl2(id,name) values(6,'11') returning id;
update tbl2 set name = '3' where id = 3;
delete from tbl2 where id = 1 and name ='1';
```

会进入串行任务执行流程，也就是小节介绍的RouterSequentialModifyExecScan函数来实现。

##### 1.4.1.1、串行修改任务执行函数RouterSequentialModifyExecScan

该函数的关键代码如下：

```c
/*
 * RouterSequentialModifyExecScan executes 0 or more modifications on a
 * distributed table sequentially and stores them in custom scan's tuple
 * store. Note that we also use this path for SELECT ... FOR UPDATE queries.
 */
static void RouterSequentialModifyExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	。。。。。。
    bool hasReturning = distributedPlan->hasReturning;//表明该DML命令是否有Returning语句
	bool multipleTasks = list_length(taskList) > 1;	 //当前执行计划任务个数是否大于1
	bool taskListRequires2PC = TaskListRequires2PC(taskList);//是否需要开启2PC提交
    bool alwaysThrowErrorOnFailure = false;//是否允许出错，即一个分片执行失败，选择副本分片继续
	。。。。。。
    //步骤1、当前CN是否需要开启事务以及是否采用2PC提交
	if (IsTransactionBlock() || multipleTasks || taskListRequires2PC ||
		StoredProcedureLevel > 0)
	{
		BeginOrContinueCoordinatedTransaction();//开启一个coordinated事务，除非已经在事务中

		/*
		 * Although using two phase commit protocol is an independent decision than
		 * failing on any error, we prefer to couple them. Our motivation is that
		 * the failures are rare, and we prefer to avoid marking placements invalid
		 * in case of failures.
		 *
		 * For reference tables, we always set alwaysThrowErrorOnFailure since we
		 * absolutely want to avoid marking any placements invalid.
		 *
		 * We also cannot handle failures when there is RETURNING and there are more
		 * than one task to execute.
		 */
		if (taskListRequires2PC)
		{   //步骤2、CN需要开启2PC            
			CoordinatedTransactionUse2PC();  //标记：当前CN事务应该采用2PC提交
			alwaysThrowErrorOnFailure = true;//标记：只要任何一个流程出错，则必须抛出错误
		}
		else if (multipleTasks && hasReturning)
		{
			alwaysThrowErrorOnFailure = true;//标记：只要任何一个流程出错，则必须抛出错误
		}
	}
    //步骤3、对执行计划涉及的各子任务，依次顺序执行
	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
        //表明是否需要储存响应结果
		bool expectResults = (hasReturning || task->relationRowLockList != NIL);
        //单一修改任务具体执行函数
		executorState->es_processed +=
			ExecuteSingleModifyTask(scanState, task, operation,
									alwaysThrowErrorOnFailure, expectResults);
	}
}
```

该函数的具体执行步骤参见代码，有几个关键点需要重点注意下：

关键点1：当该执行计划只涉及一个任务并且不需要开启2PC提交时，即该DML操作仅影响单个分片，则它将在单个工作节点内运行，因此CN不需要开启事务更加不用开启2PC提交，以提升性能。

关键点2：变量***hasReturning***，决定了该DML命令是否带有Returning语句，如果不带该语句，则程序会仅仅校验各分片返回的响应数据和操作是否成功，然后直接丢弃掉响应数据。

关键点3：变量***alwaysThrowErrorOnFailure***，决定了在一个执行任务中，任何一个placement上的操作出错，是否影响当前进程上CN事务的运行；如果为false，那么在一个分片的placement上操作失败，则执行程序会将该placement标记为无效，以保持数据一致性。而对于该执行计划涉及的表为复制表或涉及多个任务或带Returning语句或开启了2PC提交，那么该参数必定为true，也就是只要一个placement上修改失败，则当前进程上CN事务会终止并且回滚。

关键点4：变量***taskListRequires2PC***为TaskListRequires2PC函数的返回值，它决定了当前执行计划是否需要开启2PC，如果存在以下任何一种情况，则必定会开启2PC提交：

- 情况1：涉及表的复制模式为2PC提交（REPLICATION_MODEL_2PC）方式，也就是涉及的表为复制表，其中citus的复制模式（repmodel）有以下三种：

```c
#define REPLICATION_MODEL_COORDINATOR 'c' //“statement”模式
#define REPLICATION_MODEL_STREAMING 's'   //“streaming”模式
#define REPLICATION_MODEL_2PC 't'         //2PC模式
#define REPLICATION_MODEL_INVALID 'i'     //非法，未初始化
```

![1552052287866](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552052287866.png?raw=true)

<center>图5、citus复制模式<center\>

而实际上用户只能够通过“SET citus.replication_model”的方式，来选择采用“statement”（默认方式，citus自行实现的通过复制DML语句到不同的分片上达到复制的目的）还是“streaming”（pg自带的流复制方式，需要手动配置，MX架构下只有采用流复制的分布表的元数据才会被自动同步过去），至于采用2PC提交的方式是citus对于复制表强制选择的，无法修改。

- 情况2：当有些执行任务没有设置replicationModel时，根据ShardId来判断所对应的表是复制表。

- 情况3：该执行计划涉及多个task，并且配置参数multi_shard_commit_protocol为‘2pc’（默认）

> 注意：对于上面的情况3，官网文档说multi_shard_commit_protocol的默认配置是‘1pc’，而代码中却是‘2pc’

> <http://docs.citusdata.com/en/v8.1/develop/reference_dml.html>

而在步骤3中，最关键是对执行计划涉及的各子任务，依次顺序调用ExecuteSingleModifyTask函数，来对每一个修改任务进行具体执行，该函数的实现如下所示：

##### 1.4.1.2、单一修改任务具体执行函数ExecuteSingleModifyTask

在该函数中和连接相关的关键代码为：

在ExecuteSingleModifyTask函数中和连接相关的关键代码为：

```c
/*
 * ExecuteSingleModifyTask executes the task on the remote node, retrieves the
 * results and stores them, if RETURNING is used, in a tuple store. The function
 * can execute both DDL and DML tasks. When a DDL task is passed, the function
 * does not expect scanState to be present.
 *
 * If the task fails on one of the placements, the function reraises the
 * remote error (constraint violation in DML), marks the affected placement as
 * invalid (other error on some placements, via the placement connection
 * framework), or errors out (failed on all placements).
 *
 * The function returns affectedTupleCount if applicable.
 */
static int64
ExecuteSingleModifyTask(CitusScanState *scanState, Task *task, CmdType operation,
						bool alwaysThrowErrorOnFailure, bool expectResults)
{
	......
	/*
	 * Get connections required to execute task. This will, if necessary,
	 * establish the connection, mark as critical (when modifying reference
	 * table or multi-shard command) and start a transaction (when in a
	 * transaction).
	 */
	connectionList = GetModifyConnections(task, alwaysThrowErrorOnFailure);
```

如上所示，可以看到最终会调用GetModifyConnections函数来获取连接，该函数和连接相关的关键代码为：

```c
/*
 * GetModifyConnections returns the list of connections required to execute
 * modify commands on the placements in tasPlacementList.  If necessary remote
 * transactions are started.
 *
 * If markCritical is true remote transactions are marked as critical.
 */
static List *
GetModifyConnections(Task *task, bool markCritical)
{
	。。。。。。
    List *relationShardList = task->relationShardList;//若不涉及其他分片，此处为NULL
	/* first initiate connection establishment for all necessary connections */
	foreach(taskPlacementCell, taskPlacementList)
	{
		。。。。。。
		int connectionFlags = SESSION_LIFESPAN;//表明该连接有效期是session，事务结束后不释放
        。。。。。。
		ShardPlacementAccessType accessType = PLACEMENT_ACCESS_DML;
		if (task->taskType == DDL_TASK)
		{
			connectionFlags = connectionFlags | FOR_DDL//和FOR_DDL进行或运算，此时值变为6
			accessType = PLACEMENT_ACCESS_DDL;
		}
		else
		{
			connectionFlags = connectionFlags | FOR_DML;//和FOR_DML进行或运算，此时值变为10
			accessType = PLACEMENT_ACCESS_DML;
		}
        
        if (accessType == PLACEMENT_ACCESS_DDL)
		{
			/*
			 * All relations appearing inter-shard DDL commands should be marked
			 * with DDL access.
			 */
			placementAccessList =
				BuildPlacementDDLList(taskPlacement->groupId, relationShardList);
		}
		else
		{
			/* create placement accesses for placements that appear in a subselect */
           //若relationShardList为NULL，则不会给该placement新增PLACEMENT_ACCESS_SELECT操作
			placementAccessList =
				BuildPlacementSelectList(taskPlacement->groupId, relationShardList);
		}
         /* create placement access for the placement that we're modifying */
		placementModification = CreatePlacementAccess(taskPlacement, accessType);
		placementAccessList = lappend(placementAccessList, placementModification);
		/* get an appropriate connection for the DML statement */
		multiConnection = GetPlacementListConnection(connectionFlags,placementAccessList,
													 NULL);
```

上面代码关键点：

1、变量***connectionFlags***，被赋初值SESSION_LIFESPAN，若为DML操作，则该flag和FOR_DML进行或运算，最终值为10（相关枚举值含义参见2.2节），也正是因为此处的赋值，决定了后续给该insert操作分配的连接是长连接，并且是进行DML或DDL操作。

2、变量***relationShardList***，其值为task->relationShardList，它决定了该placement的placementAccessList中，除了DML外，是否还涉及了PLACEMENT_ACCESS_SELECT操作，有如下两种情形：

- 情形一、对于不带条件的简单insert，不涉及子查询，所以task->relationShardList为null，这样对于该placement，在placementAccessList中只会存在一个PLACEMENT_ACCESS_DML操作；


- 情形二、对于带“where”的update或delete，则task->relationShardList为“where”中涉及到的分片，这样该placement，在placementAccessList中除了DML操作外，还会存在一个PLACEMENT_ACCESS_SELECT。


其实这个从执行计划中就可以看出来，如下图所示：

![1552528257324](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552528257324.png?raw=true)

<center>图6、带和不带where条件的执行计划<center\>

接着调用GetPlacementListConnection函数，用于给该DML分配一个恰当的连接，该函数很简单，就两行：
```c
/*
 * GetPlacementListConnection establishes a connection for a set of placement
 * accesses.
 * See StartPlacementListConnection for details.
 */
MultiConnection *
GetPlacementListConnection(uint32 flags, List *placementAccessList, const char *userName)
{
	MultiConnection *connection = StartPlacementListConnection(flags,placementAccessList,								   userName);//复用或新建连接，用于分配给该placement
	FinishConnectionEstablishment(connection);//等待连接的创建完成，若连接创建失败，直接报错
	return connection;
}
```

上面代码关键点：

- 关键点1、placementAccessList参数，包含了当前任务中，所有涉及到的placement，以及每个placement涉及到的操作；因为此节讲述的是带分片的场景，所有每个任务中，只会涉及到一个placement，而该placement可能涉及到一到两个操作。


- 关键点2、上面调用的函数StartPlacementListConnection（后面3.1节会详细介绍），是用于给每个placement的每个操作都分配一个连接，能够复用的优先复用，不能够复用的则创建连接；只要涉及到分布表和复制表，不管是否带分片字段，最终都会调用到它。


- 关键点3、FinishConnectionEstablishment函数，是用于检测和等待上一个函数给该placementID分配的连接是否创建完成，也就是是否可以和对应的worker通信正常，通过poll()/select() 方式，如果连接失败，直接报错；如果连接超时，则直接关闭连接，释放对端资源，并且报告警。


#### 1.4.2、并行任务执行（修改操作）

当执行的DML（insert、update、delete），没有带分片字段时，如执行以下类型的语句：

```sql
--tbl2为分布表，id为分布键
update tbl2 set name = '3' where name = '3'; 
delete from tbl2 where id > 0 and id < 3; 
```

上面的SQL在执行时，会进入并行任务执行流程，也就是RouterMultiModifyExecScan函数，而它是直接调用多任务同时执行函数ExecuteModifyTasks来实现，该函数用于一个DML操作的执行计划中，多个执行task并行执行的场景，其和连接相关的关键代码为：

```c
/*
 * ExecuteModifyTasks executes a list of tasks on remote nodes, and
 * optionally retrieves the results and stores them in a tuple store.
 *
 * If a task fails on one of the placements, the transaction rolls back.
 * Otherwise, the changes are committed using 2PC when the local transaction
 * commits.
 */
static int64 ExecuteModifyTasks(List *taskList, bool expectResults, ParamListInfo                                         paramListInfo, CitusScanState *scanState)
{
	。。。。。。
	Task *firstTask = NULL;	
	int connectionFlags = 0;
    。。。。。。
    //步骤1、当前CN是否需要采用2PC提交
    if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC ||
		firstTask->replicationModel == REPLICATION_MODEL_2PC)
	{
		CoordinatedTransactionUse2PC();//标记：当前CN事务应该采用2PC提交
	}
	。。。。。。        
	if (firstTask->taskType == DDL_TASK || firstTask->taskType == VACUUM_ANALYZE_TASK)
	{
		connectionFlags = FOR_DDL;//如果是DDL任务或VACUUM ANALYZE任务，则认为是DDL操作
	}
	else
	{
		connectionFlags = FOR_DML;
	}
	/* open connection to all relevant placements, if not already open */
    //步骤2：给所有任务，涉及到的相关的每个placements，都复用或新建一个连接
	shardConnectionHash = OpenTransactionsForAllTasks(taskList, connectionFlags);
    。。。。。。
	/* iterate over placements in rounds, to ensure in-order execution */
    //步骤3：利用步骤1获取到的连接，批量发送请求，并且接收每次的响应数据，并且判断是否成功
	while (tasksPending)
	{
		。。。。。。
		/* send command to all shard placements with the current index in parallel */
        //按照任务顺序，找出每个shardId所涉及的第placementIndex个placement所分配的连接
        //并行发送命令到这些连接上面
		foreach(taskCell, taskList)
		{
			。。。。。。。
            //根据当前任务所涉及的shardId，找出上一步操作给其所包含的每个placement所分配的系列连接
			shardConnections = GetShardHashConnections(shardConnectionHash, shardId,
													   &shardConnectionsFound);
			connectionList = shardConnections->connectionList;
            。。。。。。
            //找出第placementIndex个placement所对应的连接，然后通过该连接把queryString发送出去
			connection = (MultiConnection *) list_nth(connectionList, placementIndex);
			queryOK = SendQueryInSingleRowMode(connection, queryString, paramListInfo);
			if (!queryOK)
			{
				ReportConnectionError(connection, ERROR);
			}
		}
		/* collects results from all relevant shard placements */
        //依次从上一步发送了queryString的连接中读取出结果，并且根据调用者参数，决定是否保存第一次的结果
        //当placementIndex不为0时，表明该shardId所对应的表存在副本，我们并不用保存副本上的执行结果
		foreach(taskCell, taskList)
		{
			。。。。。。
			shardConnections = GetShardHashConnections(shardConnectionHash, shardId,
													   &shardConnectionsFound);
			connectionList = shardConnections->connectionList;
            。。。。。。
			connection = (MultiConnection *) list_nth(connectionList, placementIndex);
			/*
			 * If caller is interested, store query results the first time
			 * through. The output of the query's execution on other shards is
			 * discarded if we run there (because it's a modification query).
			 */
			if (placementIndex == 0 && expectResults)
			{//保存连接中发送回的结果，并且检查是否出错
				queryOK = StoreQueryResult(scanState, connection,
										   alwaysThrowErrorOnFailure,
										   &currentAffectedTupleCount, NULL);
			}
			else
			{//仅仅从连接获取查询结果，计算行数并检查错误，但是最终会丢弃接受到的信息
				queryOK = ConsumeQueryResult(connection, alwaysThrowErrorOnFailure,
											 &currentAffectedTupleCount);
			}
            。。。。。。
		}
        。。。。。。。
	}
	。。。。。。
	return totalAffectedTupleCount;
}
```

从上可以看出，该函数主要由以下两个步骤组成：

- 步骤1：当配置参数multi_shard_commit_protocol为‘2pc’或者所涉及的表为复制表时，需要开启2PC提交。
- 步骤2：给该修改操作包含的所有任务，涉及到的每个shard，给其所有placements（如果该shard对应副本为n，则此处有n+1个placement）都复用或新建一个连接。
- 步骤3：按照任务顺序，找出每个shardId所涉及的第placementIndex个placement所分配的连接，并行发送请求到这些连接上面；然后在从这些连接上面读取出结果，使placementIndex++，然后重新该流程；若任何一个placement上的操作出错，则整个事务回滚。

在步骤2中，并不是把请求同时发送到该操作所涉及的所有placement上，而是根据shard所涉及的placements个数来批量发送，这种做法的原因是我们返回给客户端的响应结果，只需要关心master分片上修改结果，而不用关心在副本上的修改结果（会检测副本上响应结果，但是不会发送给客户端）。

比如一个表tlb6，分布个数为2并且存在一个副本，如果不带分布键删除一行数据，那么该操作就涉及到2个任务，每个任务涉及到一个shard，而每个shard又涉及到2个placement，如下图所示：

![1551779792683](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1551779792683.png?raw=true)

<center>图7、带副本的分布表执行删除操作<center\>

所以在步骤1会获取到4个连接，而在步骤2中会分两次，第一次选择placementid为136和138的，第2次选择137和139上对应连接，用其来发送请求；只会选择第一次的2个连接上的响应，发送给客户端（因为若都操作成功，第2次在副本上的操作结果也是一样的）。


注意：在涉及到含有副本的表的操作时，explain并不会看到在副本上的执行任务，若想看到可以抓包查看。

在上面步骤1给每个涉及的placement都分配一个连接的功能，主要是OpenTransactionsForAllTasks函数来实现的，其关键代码如下：

```c
/*
 * OpenTransactionsForAllTasks opens a connection for each task,
 * taking into account which shards are read and modified by the task
 * to select the appopriate connection, or error out if no appropriate
 * connection can be found. The set of connections is returned as an
 * anchor shard ID -> ShardConnections hash.
 */
HTAB *OpenTransactionsForAllTasks(List *taskList, int connectionFlags)
{
	HTAB *shardConnectionHash = NULL;
	。。。。。。
	connectionFlags |= CONNECTION_PER_PLACEMENT;//表明该流程需要给涉及的多个Placement都分配连接
	/* open connections to shards which don't have connections yet */
    //步骤1：依次遍历任务列表，对每个任务中对应的shard，若还没有分配连接，则为其复用或新建一个连接
	foreach(taskCell, taskList)
	{		
		uint64 shardId = task->anchorShardId;
        //步骤2：判断该shard是否已经被分配了连接，若已经分配则跳过
        shardConnections = GetShardHashConnections(shardConnectionHash, shardId,
												   &shardConnectionsFound);
		if (shardConnectionsFound)
		{   //若该shard已经被分配连接，则不用在分配
			continue;
		}
		//步骤3：根据shardId，从缓存（DistShardCacheHash表）中找出所有shardId相同的Placement
		shardPlacementList = FinalizedShardPlacementList(shardId);
		。。。。。。
		if (task->taskType == MODIFY_TASK)
		{
			accessType = PLACEMENT_ACCESS_DML;
		}
		else
		{
			accessType = PLACEMENT_ACCESS_DDL;
		}
        //步骤4：对上一步根据shardId找出的所包含的Placement，进行依次遍历，以便为其分配独占的连接
		foreach(placementCell, shardPlacementList)
		{
			ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(placementCell);
            List *placementAccessList = NIL;
			。。。。。。
			/* add placement access for modification */
			placementModification.placement = shardPlacement;//该Placement的信息
			placementModification.accessType = accessType;   //该Placement所涉及的操作
			placementAccessList = lappend(placementAccessList, &placementModification);
			if (accessType == PLACEMENT_ACCESS_DDL)
			{
				List *placementDDLList = BuildPlacementDDLList(shardPlacement->groupId,
															   task->relationShardList);
                。。。。。。                
				placementAccessList = list_concat(placementAccessList, placementDDLList);
			}
			else
			{
			List *placementSelectList=BuildPlacementSelectList(shardPlacement->groupId,
											 task->relationShardList);
		       //add additional placement accesses for subselects (e.g. INSERT .. SELECT)
               //如果涉及到子查询，则把子查询涉及到的placement和SELECT标志增加到待查询连接链表中  
			  placementAccessList =list_concat(placementAccessList, placementSelectList);
			}
			/*
			 * Find a connection that sees preceding writes and cannot self-deadlock,
			 * or error out if no such connection exists.
			 */
            //步骤5：给placement所涉及的操作，复用或新建一个可用连接
			connection = StartPlacementListConnection(connectionFlags,
													  placementAccessList, NULL);
            //步骤6：给该选择出的连接，设置独占标志位，即connection->claimedExclusively = true;
			ClaimConnectionExclusively(connection);
            。。。。。。
		}
	}
    。。。。。。
	return shardConnectionHash;
}
```

该函数的执行流程，可以参见代码中注释，此处需要重点关注的是：

- 关键点1：步骤1的前面connectionFlags被设置了CONNECTION_PER_PLACEMENT标志，表明该流程需要给涉及的多个Placement都分配连接，并且这些连接需要在事务结束后被释放。


- 关键点2：步骤4是对shard所包含的每个placement进行逐个遍历，对每个placement和其操作accessType都会添加到placementAccessList链表，但该链表中除了该placement外，可能还会涉到及其他相关shard的placement （相同groupId），但是涉及的操作一定是SELECT。


- 关键点3：步骤5调用StartPlacementListConnection函数，对placementAccessList链表中所有涉及的placement  和对应操作，依据一定规则最终选择出一个可用连接connection，作为步骤4对应placement的连接。（该函数的实现和选择连接的规则，参见下面的3.1节）。


- 关键点4：步骤6中会对选择出的连接connection，设置独占标志位，避免在当前事务中被其他操作所使用。


### 1.5、Router执行器之SELECT操作

当执行的SELECT操作，分布表或复制表带了分片字段、复制表间的join操作、亲和表间的join操作（必须是各自只涉及到一个shard且这些shard在一个groupId上），如执行以下类型的语句：

```sql
--tbl1和tbl2为分布表，且为亲和表，id为分片键值
select * from tbl1 where id = 1; 
select * from tbl1 where id in (select id from tbl2 where id =1);
select a.id,b.name from tbl1 a,tbl2 b where b.id=1 and a.id=1;
--tbl3和tbl4为复制表
select * from tbl3 a join tbl4 b on a.id=b.id;
select a.id,b.name from tbl3 a,tbl4 b where a.id =b.id;
```

上面的语句执行时，会进入Router执行器中的SELECT流程，即RouterSelectExecScan函数来实现，而该函数最终会调用ExecuteSingleSelectTask函数，它用于执行只有一个执行任务的查找操作，当为join时，必定涉及到的表是具有亲和关系的而且都只涉及一个shard（groupId也一样），在该函数中和连接相关的关键代码为：

```c
/*
 * ExecuteSingleSelectTask executes the task on the remote node, retrieves the
 * results and stores them in a tuple store.
 *
 * If the task fails on one of the placements, the function retries it on
 * other placements or errors out if the query fails on all placements.
 */
static void
ExecuteSingleSelectTask(CitusScanState *scanState, Task *task)
{
	。。。。。。
	/*
	 * Try to run the query to completion on one placement. If the query fails
	 * attempt the query on the next placement.
	 */
     //步骤1： 先在第一个placement上运行查询语句，若成功则退出，若发送失败，则尝试下一个placement 
	foreach(taskPlacementCell, taskPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		。。。。。。
		int connectionFlags = SESSION_LIFESPAN;//指明获取的连接在事务结束后不释放（长连接）
		。。。。。。
		if (list_length(relationShardList) > 0)
		{
            //步骤2：依次遍历该任务涉及到的shard，找出在groupId下的Placement，存放在access链表中
			placementAccessList = BuildPlacementSelectList(taskPlacement->groupId,
														   relationShardList);
		}
		else
		{
            //当该任务不涉及到任何shard时的处理，该场景很难遇到。
			。。。。。。
		}
        。。。。。。
        //步骤3：根据placementAccessList中涉及到的Placement，找出一个可用连接    
		connection = GetPlacementListConnection(connectionFlags, placementAccessList,
												NULL);
        。。。。。。
        //步骤4：通过获取的连接connection，发送查询请求queryString到该placement对应worker上
		queryOK = SendQueryInSingleRowMode(connection, queryString, paramListInfo);
		if (!queryOK)
		{   //如果发送失败，则尝试在下一个placement上发送
			continue;
		}
        //步骤5：检测收到的响应结果，若成功，则把结果保存下来，若失败，则尝试在下一个placement上发送
		queryOK = StoreQueryResult(scanState, connection, dontFailOnError,
								   &currentAffectedTupleCount,
								   &executionStats);
        //进程中间结果是否超过配置值（MaxIntermediateResult KB，默认1G），若超过，则直接报错
		if (CheckIfSizeLimitIsExceeded(&executionStats))
		{
			ErrorSizeLimitIsExceeded();
		}

		if (queryOK)
		{   //若操作成功，则直接返回
			return;
		}
	}
	ereport(ERROR, (errmsg("could not receive query results")));
}
```

该函数的具体执行步骤，参见代码中的注释，需要注意以下几个关键点：

- 关键点1：若该SELECT操作涉及的shard存在副本，则在第一个placement上运行查询出错后，就会选择其他副本对应的placement，若所有副本也都运行失败，则报错。


- 关键点2：步骤2前面的connectionFlags，初值为SESSION_LIFESPAN，表明给placement获取的连接在事务结束后不释放，该连接将被标识为长连接。


- 关键点3：步骤3中调用GetPlacementListConnection函数，来给placement获取一个可用连接，该函数的实现在前面1.1节的末尾介绍过。


- 关键点4：若查询到的结果中，中间结果超过配置值（MaxIntermediateResult KB，默认1G），则直接报错并且退出，可用通过set citus.max_intermediate_result_size命令来调大。


### 1.6、Real Time执行器的执行方法

当执行的SELECT操作：分布表或复制表没有带分片字段的查询操作、亲和表间的join操作、分布表和复制表间的join操作，如执行以下类型的语句：

```sql
--tbl1和tbl2为分布表，且为亲和表，id为分片键值
select * from tbl1 where name = '1';
select * from tbl1 where id in (select id from tbl2 where id >1);
select a.id,b.name from tbl1 a,tbl2 b where b.id=a.id
--tbl3
select * from tbl3 a join tbl1 b on a.id=b.id
select a.id,b.name from tbl3 a,tbl1 b where a.id =b.id;
```

上面的语句执行时，会进入Real Time执行器的查询执行流程，即RealTimeExecScan函数来实现，该函数是一个回调函数，主要功能是从Real Time执行器的执行结果中返回下一个元组数据，其关键源码为：

```c
/*
 * RealTimeExecScan is a callback function which returns next tuple from a real-time
 * execution. In the first call, it executes distributed real-time plan and loads
 * results from temporary files into custom scan's tuple store. Then, it returns
 * tuples one by one from this tuple store.
 */
TupleTableSlot *RealTimeExecScan(CustomScanState *node)
{
	//功能1：依次递归执行各个执行计划，并且把执行计划获取到的数据保存到tuplestore中
	if (!scanState->finishedRemoteScan)
	{
		/* we are taking locks on partitions of partitioned tables */
        //对涉及的分区表中的分区进行锁定
		LockPartitionsInRelationList(distributedPlan->relationIdList, AccessShareLock);
        //在当前节点创建一个目录，用于存放执行结果，并且该目录在portaldelete时自动释放
		PrepareMasterJobDirectory(workerJob);
        //递归执行子计划
		ExecuteSubPlans(distributedPlan);
        //调用RealTime执行器的执行方法，执行当前执行计划
		MultiRealTimeExecute(workerJob);
        //首先创建一个tuplestore，然后将保存了执行结果的这些文件，逐个复制到tuplestore中
		LoadTuplesIntoTupleStore(scanState, workerJob);
        //标记功能1中会仅仅执行一次
		scanState->finishedRemoteScan = true;
	}
    //功能2：从tuplestore中读取下一个tuple并且返回 
	resultSlot = ReturnTupleFromTuplestore(scanState);
	return resultSlot;
}
```

该函数实现的主要功能有两个，参见代码注释，其中有几个关键点：

- 关键点1：该函数在第一次进入时，会执行分布式执行计划，并将每次执行结果所保存的临时文件加载到tuplestore中。 

- 关键点2：每次调用时，都会从tuplestore中逐个返回元组数据；也就是说如果执行结果含有N个元组，那么就会调用N+1次。

从上面的代码可以看出，在第一次调用时，就会运行当前执行计划，执行函数为MultiRealTimeExecute，它会不停的循环当前执行计划涉及的各个任务，直到所有任务完成或一个任务失败或用户主动取消，该部分的源码如下：

```c
/*
 * MultiRealTimeExecute loops over the given tasks, and manages their execution
 * until either one task permanently fails or all tasks successfully complete.
 * The function opens up a connection for each task it needs to execute, and
 * manages these tasks' execution in real-time.
 */
void MultiRealTimeExecute(Job *job)
{
	。。。。。。
	/* loop around until all tasks complete, one task fails, or user cancels */
    //步骤1：不停的循环当前执行计划涉及的各个任务，直到所有任务完成或一个任务失败或用户主动取消
	while (!(allTasksCompleted || taskFailed || QueryCancelPending ||
			 sizeLimitIsExceeded))
	{		
		。。。。。。
        //步骤2：遍历当前执行计划涉及的各个任务，并且逐个执行
		forboth(taskCell, taskList, taskExecutionCell, taskExecutionList)
		{
			Task *task = (Task *) lfirst(taskCell);
			。。。。。。
			//步骤3：如果当前任务已经开始或达到当前工作进程配置的最大连接数或文件描述符数，则不往后执行
			if (TaskExecutionReadyToStart(taskExecution) &&
				(WorkerConnectionsExhausted(workerNodeState) ||
				 MasterConnectionsExhausted(workerHash)))
			{
				continue;
			}

			/* call the function that performs the core task execution logic */
            //步骤4：调用核心的任务执行逻辑函数
			connectAction = ManageTaskExecution(task, taskExecution, &executionStatus,
												&executionStats);

			/* update the connection counter for throttling */
            //步骤5：更新连接计数器,以便进行连接限制
			UpdateConnectionCounter(workerNodeState, connectAction);			
            //步骤6：若当前任务失败，需要主动清除其客户端资源，并迭代后续执行任务。故这里记录失败而不是立即报错
			taskFailed = TaskExecutionFailed(taskExecution);
			if (taskFailed)
			{
				failedTaskId = taskExecution->taskId;
				break;
			}
            //步骤7：检查当前 执行计划是否完成
			taskCompleted = TaskExecutionCompleted(taskExecution);
			if (taskCompleted)
			{
				completedTaskCount++;
			}
			else
			{
				。。。。。。
                //如果任务尚未完成，则记下此任务，以及正在等待的响应的对应连接。
				MultiClientRegisterWait(waitInfo, executionStatus, connectionId);
			}
		}
		/* in case the task has intermediate results */
        //检查中间结果所占空间是否超过配置值
		if (CheckIfSizeLimitIsExceeded(&executionStats))
		{
			sizeLimitIsExceeded = true;//若成功，则标记状态，并且结束当前执行计划
			break;
		}
		
        //步骤8：检查所有任务是否执行完成
		if (completedTaskCount == taskCount)
		{
			allTasksCompleted = true;
		}
		else
		{   //如果有执行计划还未完成，则等待，直到全部完成，则停止等待sockets
			MultiClientWait(waitInfo);
		}
        。。。。。。
	}
    。。。。。。
	/* cancel any active task executions */
    //步骤9：处理用户主动取消（cancel）场景
	taskExecutionCell = NULL;
	foreach(taskExecutionCell, taskExecutionList)
	{
		TaskExecution *taskExecution = (TaskExecution *) lfirst(taskExecutionCell);
		CancelTaskExecutionIfActive(taskExecution);
	}
	。。。。。
	/* 步骤10：close connections and open files  */	
	foreach(taskExecutionCell, taskExecutionList)
	{
		TaskExecution *taskExecution = (TaskExecution *) lfirst(taskExecutionCell);
		CleanupTaskExecution(taskExecution);
	}
    。。。。。。
}
```

该函数的主要执行流程可以参见源码中的注释，需要注意的是：

- 关键点1：在步骤1可以看出它是一个循环，每次循环都会遍历该执行计划涉及的所有task，直到所有task成功完成或一个task执行失败或用户主动取消才会终止该循环。

- 关键点2：在步骤2中，他会逐个遍历当前执行计划涉及到的各task，若某个task在发送请求后，还没有收到结果，此时不会等待，而是在步骤6中，把该task分配的连接所对应的socket保存下来，待所有task都遍历完一次后，在统一进行等待，提升并行效率。

- 关键点3：在步骤3中会在给每个task执行前，都会检查当前已经分配出的连接数和文件描述符数，是否达到配置的最大连接数或文件描述符数参数。

- 注意点4：在步骤5中，若当前执行任务失败，仅仅只是记录当前失败而不是立即报错，因为还需要迭代后续的执行计划，此时会停止步骤1中的循环，以便尽快结束当前执行任务，转而执行下一个执行任务。

在步骤4主，会调用核心的任务执行逻辑函数ManageTaskExecution，该函数会管理给定task的所有执行逻辑， 为此，该函数在所涉及的节点上启动一个新的“执行”，并跟踪该执行的进度。 失败时，该函数在分片副本（若有）所在的另一个节点上重新启动此执行。需要注意的是该函数实际上是一个状态机，根据每一步的执行情况决定下一步如何运行，状态机的各个状态如下：

```c
/* Enumeration to track one task's execution status */
typedef enum
{
	EXEC_TASK_INVALID_FIRST = 0,
	EXEC_TASK_CONNECT_START = 1,
	EXEC_TASK_CONNECT_POLL = 2,
	EXEC_TASK_FAILED = 3,
	EXEC_COMPUTE_TASK_START = 4,
	EXEC_COMPUTE_TASK_RUNNING = 5,
	EXEC_COMPUTE_TASK_COPYING = 6,
	EXEC_TASK_DONE = 7,

	/* used for task tracker executor */
	EXEC_TASK_UNASSIGNED = 11,
	EXEC_TASK_QUEUED = 12,
	EXEC_TASK_TRACKER_RETRY = 13,
	EXEC_TASK_TRACKER_FAILED = 14,
	EXEC_SOURCE_TASK_TRACKER_RETRY = 15,
	EXEC_SOURCE_TASK_TRACKER_FAILED = 16,

	/* transactional operations */
	EXEC_BEGIN_START = 20,
	EXEC_BEGIN_RUNNING = 21
} TaskExecStatus;
```

 因为ManageTaskExecution函数涉及到各个状态机的转换，所以函数的实现在此处不仔细讲述，只是需要注意的是对于RealTime执行器，状态机的初始状态为EXEC_TASK_CONNECT_START，在该状态下，会给该任务涉及到的每个Placement都分配一个连接，完成该任务的是MultiClientPlacementConnectStart函数，实现如下：

```c
/*
 * MultiClientPlacementConnectStart asynchronously tries to establish a connection
 * for a particular set of shard placements. If it succeeds, it returns the
 * the connection id. Otherwise, it reports connection error and returns
 * INVALID_CONNECTION_ID.
 */
int32
MultiClientPlacementConnectStart(List *placementAccessList, const char *userName)
{
	。。。。。。
    //步骤1：从连接池中找出一个未使用的connectionId
    int32 connectionId = AllocateConnectionId();
	int connectionFlags = 0;
	
	if (MultiShardConnectionType == PARALLEL_CONNECTION)
	{   //如果配置值multi_shard_modify_mode为‘parallel’，则表明需要给每个placement分配一个连接
		connectionFlags = CONNECTION_PER_PLACEMENT;//flag添加CONNECTION_PER_PLACEMENT标志位
	}
    。。。。。。
	/* prepare asynchronous request for worker node connection */
    //步骤2：给所有涉及的placement分配一个到对应worker节点的连接
	connection = StartPlacementListConnection(connectionFlags, placementAccessList,
											  userName);
    //步骤3：给该当前选择出的连接，设置独占标志位，即connection->claimedExclusively = true;
	ClaimConnectionExclusively(connection);
    //步骤4：检测连接是否创建成功
	connStatusType = PQstatus(connection->pgConn);
	if (connStatusType != CONNECTION_BAD)
	{   //若连接创建成功，则将该连接保存到连接池，并且设置连接初试状态为PGRES_POLLING_WRITING
		ClientConnectionArray[connectionId] = connection;
		ClientPollingStatusArray[connectionId] = PGRES_POLLING_WRITING;
	}
	else
	{   //若连接没有创建成功，直接告知该连接不可用
		ReportConnectionError(connection, WARNING);
		connectionId = INVALID_CONNECTION_ID;
	}
	return connectionId;
}
```

该函数的实现步骤参见源码中的注释，需要注意的是：

- 注意点1：如果环境配置值multi_shard_modify_mode为‘parallel’，则表明需要给每个placement分配一个连接，则会给flag添加CONNECTION_PER_PLACEMENT标志位。

- 注意点2：在步骤3，会给该当前选择出的连接，设置独占标志位，即claimedExclusively为true，防止当前事务该连接被复用。

- 关键点3：在步骤4，若连接创建成功，则将该连接保存到连接池ClientConnectionArray中，以便进行并行操作，也就是给当前执行计划设计的每个task都先获取连接，然后在发送请求。

在步骤2会调用StartPlacementListConnection函数，用于获取一个可以连接，该函数的实现参见下面的3.1节。

## 二、关键数据结构

本章节主要先介绍下citus源码中和连接相关的关键数据结构，方便后面章节的使用。

### 2.1、节点连接池管理hash表ConnectionHash

ConnectionHash哈希表的定义和其对应的hash key和hash entry如下：

```c
/*
 * Central connection management hash, mapping (host, port, user, database) to
 * a list of connections.
 *
 * This hash is used to keep track of which connections are open to which
 * node. Besides allowing connection reuse, that information is e.g. used to
 * handle closing connections after the end of a transaction.
 */
/* the hash table */
extern HTAB *ConnectionHash;

/* hash key */
typedef struct ConnectionHashKey
{
	char hostname[MAX_NODE_LENGTH];
	int32 port;
	char user[NAMEDATALEN];
	char database[NAMEDATALEN];
} ConnectionHashKey;

/* hash entry */
typedef struct ConnectionHashEntry
{
	ConnectionHashKey key;
	dlist_head *connections; //类型为1.2节中介绍的MultiConnection结构
} ConnectionHashEntry;
```

从上可以看出，ConnectionHash是一个保存节点相关连接的hash表，其中是以worker上的主机名、端口号、连接的用户名和数据库名作为一个key，来保存所有该postgres子进程所创建的连接；用于跟踪哪些节点上创建了哪些连接，该hash表中的信息除了用于连接重用之外，还能够用于在事务结束后处理相关连接的正常关闭。

关键点：

1、该hash表的key是 (host, port, user, database)，所以是和分片无关的，故对于一个客户端，只要其没有和CN断开连接，也没有切换数据库和用户，那么它所发送的所有SQL，在涉及到同一个worker时，只要该key对应的hash entry的连接链表connections中，存在可用连接（即claimedExclusively为false），则这些连接都可以直接复用，而不用管分片。

2、该hash表中，每个key所对应的值，是一个连接链表，因为对于一个worker，可能涉及到不止一个连接

3、该hash表中的值，只有该postgres子进程退出时，才会被释放，这也是为啥其中的连接能够被重用（连接标志sessionLifespan值为false时，在事务结束后会被释放）。

### 2.2、连接信息关键结构

和连接信息相关的数据结构中，有两个结构至关重要，分别是：MultiConnectionMode和MultiConnection，其中MultiConnectionMode是一个枚举值，用于标记连接的建立状态，而MultiConnection是一个结构体，包含了一个连接的详细信息，这两个数据结构如下所示：

```c
/*
 * Flags determining connection establishment behaviour.
 */
enum MultiConnectionMode
{
	/* force establishment of a new connection */
	FORCE_NEW_CONNECTION = 1 << 0, //强制新建连接，避免连接复用

	/* mark returned connection as having session lifespan */
	SESSION_LIFESPAN = 1 << 1,    //表明该连接的生命周期是整个session

	FOR_DDL = 1 << 2,             //表明该连接正在进行DDL操作

	FOR_DML = 1 << 3,             //表明该连接正在进行DML操作

	/* open a connection per (co-located set of) placement(s) */
	CONNECTION_PER_PLACEMENT = 1 << 4 //表明需要为每个所涉及的分片都分配一个连接
};
/* declaring this directly above makes uncrustify go crazy */
typedef enum MultiConnectionMode MultiConnectionMode;
```

在上面MultiConnectionMode的枚举定义中，可以看出，该结构指明了是否需要新建连接、连接的生命周期是否是整个session、该连接是否被分配为用于执行DDL或DML操作、是否需要为每个所涉及的分片都分配一个连接。

关键点：

1、在实际使用中，是对其中的枚举值复合使用的，也就是一个flag中可能包含多个值，比如当delete指定了分片字段时，flag为10（即：FOR_DML | SESSION_LIFESPAN）；当delete进行范围删除时，flag为24（即：FOR_DML | CONNECTION_PER_PLACEMENT）；

2、citus在进行SELECT或DML操作时，一般都是尽量复用已有连接，只有进行DDL操作时，一般才会设置FORCE_NEW_CONNECTION标志位。

```c
typedef struct MultiConnection
{
	/* connection details, useful for error messages and such. */
	char hostname[MAX_NODE_LENGTH];
	int32 port;
	char user[NAMEDATALEN];
	char database[NAMEDATALEN];

	/* underlying libpq connection */
	struct pg_conn *pgConn; //底层libpq连接

	/* is the connection intended to be kept after transaction end */
	bool sessionLifespan; //决定该连接的生命周期是否是整个session，即事务结束后要保留的连接

	/* is the connection currently in use, and shouldn't be used by anything else */
	bool claimedExclusively; //标记该连接是否是当前正在使用的连接，为ture时不应该被复用

	/* time connection establishment was started, for timeout */
	TimestampTz connectionStart;

	/* membership in list of list of connections in ConnectionHashEntry */
	dlist_node connectionNode;

	/* information about the associated remote transaction */
	RemoteTransaction remoteTransaction;

	/* membership in list of in-progress transactions */
	dlist_node transactionNode;

	/* list of all placements referenced by this connection */
	dlist_head referencedPlacements;//保存哪些placements使用了这个连接

	/* number of bytes sent to PQputCopyData() since last flush */
	uint64 copyBytesWrittenSinceLastFlush;
} MultiConnection;
```

在上面的MultiConnection结构体中，包含了一条连接的详细详细，比如该连接相关连的主机名、端口号、用户名和数据库名字；也包含了一个libpq连接，我们数据的发送和接收，都是用该结构进行的；同样还包含了连接的生命周期、连接是否已经被占用、连接创建的时间等相关信息。

关键点：

1、该结构体的成员sessionLifespan，直接决定了一个连接在事务完成 后是否被释放，也正是因为它，才是citus的连接有了长连接和短连接的区别；连接释放的函数参见AfterXactHostConnectionHandling；

2、该结构体的成员claimedExclusively，直接决定了某个连接是否已经被分配出去，并且正在被其他组件所独占，也正是因为它，才使得citus存在了一个限制，即在单一事务中涉及多个分片操作的DML语句时存在问题，后面将消息叙述其原因。

### 2.3、分片位置连接管理hash表ConnectionPlacementHash

ConnectionPlacementHash哈希表的定义和其对应的hash key和hash entry如下：

```c
/*
 * Hash table mapping placements to a list of connections.
 *
 * This stores a list of connections for each placement, because multiple
 * connections to the same placement may exist at the same time. E.g. a
 * real-time executor query may reference the same placement in several
 * sub-tasks.
 *
 * We keep track about a connection having executed DML or DDL, since we can
 * only ever allow a single transaction to do either to prevent deadlocks and
 * consistency violations (e.g. read-your-own-writes).
 */
/* hash table */
static HTAB *ConnectionPlacementHash;

/* hash key */
typedef struct ConnectionPlacementHashKey
{
	uint64 placementId;
} ConnectionPlacementHashKey;

/* hash entry */
typedef struct ConnectionPlacementHashEntry
{
	ConnectionPlacementHashKey key;

	/* did any remote transactions fail? */
	bool failed;

	/* primary connection used to access the placement */
	ConnectionReference *primaryConnection; //存放连接详细信息

	/* are any other connections reading from the placements? */
    //是否有多个连接正在对该placement进行读操作，如果为true，则该连接后续不能进行DDL操作
	bool hasSecondaryConnections;

	/* entry for the set of co-located placements */
	struct ColocatedPlacementsHashEntry *colocatedEntry;//该Placement对应的亲和连接hash表实例

	/* membership in ConnectionShardHashEntry->placementConnections */
	dlist_node shardNode;
} ConnectionPlacementHashEntry;
```

从上面代码可以看出，ConnectionPlacementHash是一个保存了给每个分片（placementId）所分配连接的hash表，它是以placementId作为key，来保存所有为该placementId所创建的连接；

名词解释：placementId和shardid都是用来标识具体分片的，但是他们是有区别的，placementId是指代每个独立分片的，它的值是从1开始递增的，整个citus集群中，该值是唯一的，而shardid是指代具有相同数据分片的，当一个表具有多个副本（即citus.shard_replication_factor不为1）时，shardid是会出现重复的，主分片和所有副本分片共用同一个shardid；

如下图所示，创建了一个表tbl3，他的分片个数为3，有一个副本（citus.shard_replication_factor=2）：

![1551238061304](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1551238061304.png?raw=true)

<center>图8、placementId和shardid图<center\>

附：其实也存在一个以shardid作为key的hash表：ConnectionShardHash，它保存了和每个分片（shardid）相关联的所有连接，因为和本文所讲内容无关，所有不扩展。


关键点：

1、该哈希表，在每个事务完成后，其中的值都会被清空（参见ResetPlacementConnectionManagement函数），所以在每个事务中，所有涉及到的placementId，都需要被重新分配连接，并且保存到该哈希表中；

2、如果在一个事务中，某个placementId在之前已经分配了连接，并且flag没有FORCE_NEW_CONNECTION位，那么都会尽量复用已经分配的连接（唯一的例外是不带分布键，涉及到同一个worker上非亲和表的多条SQL, 比如：tbl1和tbl2是非亲和分布表，在一个事务中，先对tbl1执行了带分片的操作，接着又对tbl2执行了不带分片的操作，此时对tbl2涉及的分片所分配的连接，无法复用tbl1已经分配的连接）；

3、citus获取连接，是按照placementId来的，这样就可能会存在对该placementId，既要进行SELECT操作，又要进行DML或DDL操作，而每个操作，都需要分配一个连接，大部分场景下都是复用同一个连接，也有一些join操作，被分配了多个连接。

4、在该哈希表的实例ConnectionPlacementHashEntry中hasSecondaryConnections成员，是用于标记一条SQL是否同时分配了多个连接对该placement进行操作，并且其值为true时，可以明确表明之前的连接为读操作，这是由citus的连接复用机制决定，如果一个事务中，之前对此placementId的操作是非SELECT操作，那么必定后续涉及到该placementId的操作都得复用他。另外其值如果为true，避免死锁，则后续不能在该连接上进行DDL操作，因为已经同时有多个连接操作在该placementId上(typically as a result of a reference table join) ；

### 2.4、亲和位置连接管理hash表ColocatedPlacementsHash

ConnectionPlacementHash哈希表的定义和其对应的hash key和hash entry如下：

```c
/*
 * A hash-table mapping colocated placements to connections. Colocated
 * placements being the set of placements on a single node that represent the
 * same value range. This is needed because connections for colocated
 * placements (i.e. the corresponding placements for different colocated
 * distributed tables) need to share connections.  Otherwise things like
 * foreign keys can very easily lead to unprincipled deadlocks.  This means
 * that there can only one DML/DDL connection for a set of colocated
 * placements.
 *
 * A set of colocated placements is identified, besides node identifying
 * information, by the associated colocation group id and the placement's
 * 'representativeValue' which currently is the lower boundary of it's
 * hash-range.
 *
 * Note that this hash-table only contains entries for hash-partitioned
 * tables, because others so far don't support colocation.
 */
static HTAB *ColocatedPlacementsHash;

/* hash key */
typedef struct ColocatedPlacementsHashKey
{
	/* to identify host - database can't differ */
	char nodeName[MAX_NODE_LENGTH];
	uint32 nodePort;

	/* colocation group, or invalid */
	uint32 colocationGroupId; //亲和组id，也就是用来识别亲和表的，同类型亲和表，值是一样的

	/* to represent the value range */
	uint32 representativeValue;//该值为hash分布时，识别一个分片的hash范围的最小值
} ColocatedPlacementsHashKey;

/* hash entry */
typedef struct ColocatedPlacementsHashEntry
{
	ColocatedPlacementsHashKey key;

	/* primary connection used to access the co-located placements */
	ConnectionReference *primaryConnection;//存放连接详细信息

	/* are any other connections reading from the placements? */
    //是否有多个连接正在对该亲和位置进行读操作，如果为true，则该连接后续不能进行DDL操作
	bool hasSecondaryConnections;
}  ColocatedPlacementsHashEntry;
```

从上面代码可以看出，ColocatedPlacementsHash是一个保存了每个亲和位置（colocationGroupId）所相关联连接的hash表，它是以主机名、端口号、colocationGroupId以及representativeValue联合作为key，来保存所有为该亲和位置所创建的连接；

名词解释：

1、***colocationGroupId***：亲和表id，也就是在元数据pg_dist_partition中的colocationid，用于识别不同表是否是亲和表，同类型亲和表，其值是一样的；该值的取值是从1开始递增的，在创建分布表时，若shard_count和shard_replication_factor，与之前创建的表一样时，那么这两个表就是亲和表，colocationid也是一样。

2、***representativeValue***：某个分片的hash取值范围的下限值（也就是下图中的shardminvalue），通过该值可以识别两个亲和表的分片是否具有相同的hash范围，这是因为如果是亲和表，那么必然是具有相同的分片数，而且每个分片的hash取值范围也是一样的，如下图所示：

![1551257730883](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1551257730883.png?raw=true)

<center>图9、亲和表和非亲和表的元数据<center\>

从上图可以看出，tbl1和tbl2是亲和表，所以四个分片的取值范围是一样的，而tbl3和他们不是亲和关系，但是也可能会出现某个分片的shardminvalue一样，即representativeValue一样，但是colocationid不一样；


也正是因为这样，决定了在一个事务中，若有个SQL1对tbl1的102008分片进行操作，创建了一个连接connect1 （该连接会保存到ColocatedPlacementsHash表中），紧接着的SQL2对tbl2的102012分片进行操作，那么就会从ColocatedPlacementsHash表中找到之前的连接connect1，然后复用它，而若是SQL3对tbl3的102016分片进行操作，因为colocationid不同，所以无法在该hash表中找到连接，只能够重新创建（注此处列举的三个分片都在同一个worker上）。

关键点：

1、亲和表间，在同一个worker上具有相同shardminvalue值的两个分片间，连接是可以复用的，比如上面举例说明的102008分片和102012分片。

2、该哈希表，在每个事务完成后，其中的值都会被清空（参见ResetPlacementConnectionManagement函数），所以在每个事务中，所有涉及到的亲和表连接，都需要被重新分配连接，并且保存到该哈希表中；

3、给每个分片位置placementId分配连接的函数FindOrCreatePlacementEntry中，会优先根据placementId从ConnectionPlacementHash表中查找是否存在连接，若有则复用，若没有则再从ColocatedPlacementsHash表中查找，若也没有找到，则新建。

## 三、关键函数之连接选择

只要所涉及的操作是分布表或复制表，则获取连接的流程基本相同，本章节主要介绍citus选择一个可用连接时所涉及的相关函数和步骤。

### 3.1、分配连接StartPlacementListConnection

StartPlacementListConnection函数，完成的功能可以分为三个步骤：

步骤1、调用FindPlacementListConnection函数，从2.3节和2.4节介绍的两个hash表中依次查找是否存在可以复用连接，若找到则复用，函数执行完会调至步骤3，若找不到，则先在对应的hash表中插入一个记录，假设已经分配了一个连接（该连接在步骤2中会新建）。

步骤2、因为在那两个和分片相关的hash表中，没有可复用连接，所以调用StartNodeUserDatabaseConnection函数，用于从和节点相关的hash表（参见2.1节）中查找是否存在连接，若找到则复用，若找不到则会调用函数StartConnectionEstablishment，来新建一个连接，该步骤的详细介绍参见后面的3.4节。

步骤3、梳理给每个placement所分配的连接（可能是步骤1从两个分片相关的hash表中复用的）和前面两个步骤最终选择的连接chosenConnection，是否一致，若一致，则不做处理；若不一致，则需要按照情况进行选择，因为最终每一个placement，都只能够被分配一个连接，该步骤的详细介绍参见后面的3.5节。

StartPlacementListConnection函数的代码，如下所示（因为后面会对上面的三个步骤进行详细描述，所以此处略去了代码细节）：

```c
/*
 * StartPlacementListConnection returns a connection to a remote node suitable for
 * a placement accesses (SELECT, DML, DDL) or throws an error if no suitable
 * connection can be established if would cause a self-deadlock or consistency
 * violation.
 */
MultiConnection *
StartPlacementListConnection(uint32 flags, List *placementAccessList,
							 const char *userName)
{
	。。。。。
    //步骤1：
	chosenConnection = FindPlacementListConnection(flags, placementAccessList, userName,
												   &placementEntryList);
     //步骤2：
	if (chosenConnection == NULL)
	{
		。。。。。。
		chosenConnection = StartNodeUserDatabaseConnection(flags, nodeName, nodePort,
														   userName, NULL);
        。。。。。。
	}
	/*
	 * Now that a connection has been chosen, initialise or update the connection
	 * references for all placements.
	 */
     //步骤3：
	forboth(placementAccessCell, placementAccessList,
			placementEntryCell, placementEntryList)
	{	
        。。。。。。
		if (placementConnection->connection == chosenConnection)
		{
			/* using the connection that was already assigned to the placement */
		}
		else if (placementConnection->connection == NULL)
		{
			。。。。。。
		}
		else
		{
			。。。。。。
		}
        。。。。。。
	}
     。。。。。。
	return chosenConnection;
}
```

### 3.2、复用缓存中的分片连接FindPlacementListConnection

函数FindPlacementListConnection，用于根据Placement将要执行的操作（SELECT、DML、DDL），来决定一个已经存在的连接，是否能够分配给该Placement；一个查找出的已经存在的连接placementEntry或者亲和连接colocatedEntry，能否被复用，存在如下限制：

限制1、将要执行DDL操作，则该连接的placementEntry->hasSecondaryConnections不能够为true（参见1.3节关键点4），原因是若已经有多个read操作（未提交）的连接都分配在该Placement上，而此时又要在该Placement上执行DDL操作，则有可能出现self-deadlock（比如复制表和分布表间的join的结果，作为对该复制表进行DDL的条件）。

```sql
--tbl1是分布表（分片个数为4个，worker为2个）、tbl3是复制表
begin;
select *  from tbl3 a join tbl1 b on a.id=b.id;
drop table tbl3;
rollback;
```

限制2、将要执行DDL操作，则该Placement找到的亲和连接上colocatedEntry->hasSecondaryConnections也不能够为true，原因是若该Placement和一些其对应的亲和分片间存在某些关系（比如外键），而此时在其找到的亲和连接对应的分片上，已经有多个read操作（未提交）的连接，而此时又要在该Placement上执行DDL操作，则有可能出现self-deadlock。

```sql
--tbl1是分布表（分片个数为4个，worker为2个）、tbl3是复制表、tbl5是本地表但是想变为复制表
begin;
create table tbl5 as select a.id,b.name from tbl1 a,tbl3 b where a.id =b.id;
SELECT create_reference_table('tbl5');
rollback;
```

限制3、对于之前已经找到的连接chosenConnection，而该连接又是在进行DML或DDL操作，那么当前操作必须复用该连接，若无法复用则报错。

限制4、对于当前找到的连接placementEntry，必须是没有被其他模块所占用，也就是claimedExclusively不能够为true，而且flag不能够含有FORCE_NEW_CONNECTION标志。

限制5、对于当前找到的连接placementEntry，若该连接之前进行的是DML或DDL操作，则必须复用他，若因为限制4的原因无法被复用，则报错。

```sql
 --tbl2是分布表，分片个数为4个，worker为2个
 begin;
 insert into tbl2(id,name) values(8,'11'),(6,'11');
 update tbl2 set name = '3' where name = '3';
 rollback;
```

限制6、对于当前找到的连接placementEntry，若该连接之前进行的是SELECT操作，而当前将要执行的操作是DDL，若因为限制4的原因无法被复用，则报错。

限制7、对于当前找到的连接placementEntry，若该连接之前进行的是SELECT操作，而当前将要执行的操作是SELECT，则不复用。

需要注意的是，上面的七种限制，是根据代码来叙述的，如下所示（去掉了一些不影响阅读的代码细节）：

```c
/*
 * FindPlacementListConnection determines whether there is a connection that must
 * be used to perform the given placement accesses.
 *
 * If a placement was only read in this transaction, then the same connection must
 * be used for DDL to prevent self-deadlock. If a placement was modified in this
 * transaction, then the same connection must be used for all subsequent accesses
 * to ensure read-your-writes consistency and prevent self-deadlock. If those
 * conditions cannot be met, because a connection is in use or the placements in
 * the placement access list were modified over multiple connections, then this
 * function throws an error.
 *
 * The function returns the connection that needs to be used, if such a connection
 * exists, and the current placement entries for all placements in the placement
 * access list.
 */
static MultiConnection *
FindPlacementListConnection(int flags, List *placementAccessList, const char *userName,
							List **placementEntryList)
{
	。。。。。。
	/*
	 * Go through all placement accesses to find a suitable connection.
	 *
	 * If none of the placements have been accessed in this transaction, connection
	 * remains NULL.
	 *
	 * If one or more of the placements have been modified in this transaction, then
	 * use the connection that performed the write. If placements have been written
	 * over multiple connections or the connection is not available, error out.
	 *
	 * If placements have only been read in this transaction, then use the last
	 * suitable connection found for a placement in the placementAccessList.
	 */
	foreach(placementAccessCell, placementAccessList)
	{
		。。。。。。
        //两个和分片相关的hash表中，依次查找是否存在可复用的连接
		placementEntry = FindOrCreatePlacementEntry(placement);
        colocatedEntry = placementEntry->colocatedEntry;//存放其亲和关系相关连接
		placementConnection = placementEntry->primaryConnection;//当前Placement可复用连接
		/* note: the Asserts below are primarily for clarifying the conditions */
		if (placementConnection->connection == NULL)
		{
			/* no connection has been chosen for the placement */
            //如果没有找到可复用连接，则继续为下一个操作选择连接
		}
        //限制1
		else if (accessType == PLACEMENT_ACCESS_DDL &&
				 placementEntry->hasSecondaryConnections)
		{
			/*
			 * If a placement has been read over multiple connections (typically as
			 * a result of a reference table join) then a DDL command on the placement
			 * would create a self-deadlock.
			 */
            。。。。。。
			ereport(ERROR,	(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot perform DDL on placement " UINT64_FORMAT
							", which has been read over multiple connections",
							placement->placementId)));
		}
        //限制2
		else if (accessType == PLACEMENT_ACCESS_DDL && colocatedEntry != NULL &&
				 colocatedEntry->hasSecondaryConnections)
		{
			/*
			 * If a placement has been read over multiple (uncommitted) connections
			 * then a DDL command on a co-located placement may create a self-deadlock
			 * if there exist some relationship between the co-located placements
			 * (e.g. foreign key, partitioning).
			 */
            。。。。。。
			ereport(ERROR,(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("cannot perform DDL on placement " UINT64_FORMAT
				" since a co-located placement has been read over multiple connections",
							placement->placementId)));
		}
        //限制3
		else if (foundModifyingConnection)
		{
			/*
			 * We already found a connection that performed writes on of the placements
			 * and must use it.
			 */
			if ((placementConnection->hadDDL || placementConnection->hadDML) &&
				placementConnection->connection != chosenConnection)
			{
				/*
				 * The current placement may have been modified over a different
				 * connection. Neither connection is guaranteed to see all uncomitted
				 * writes and therefore we cannot proceed.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						 errmsg("cannot perform query with placements that were "
								"modified over multiple connections")));
			}
		}
        //限制4
		else if (CanUseExistingConnection(flags, userName, placementConnection))
		{
			/*
			 * There is an existing connection for the placement and we can use it.
			 */
            。。。。。。
			chosenConnection = placementConnection->connection;
			if (placementConnection->hadDDL || placementConnection->hadDML)
			{
				/* this connection performed writes, we must use it */
				foundModifyingConnection = true;
			}
		}
        //限制5
		else if (placementConnection->hadDDL)
		{
			/*
			 * There is an existing connection, but we cannot use it and it executed
			 * DDL. Any subsequent operation needs to be able to see the results of
			 * the DDL command and thus cannot proceed if it cannot use the connection.
			 */
            。。。。。。
			ereport(ERROR,(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				   errmsg("cannot establish a new connection for "
						  "placement " UINT64_FORMAT
						  ", since DDL has been executed on a connection that is in use",
							placement->placementId)));
		}
        //限制5
		else if (placementConnection->hadDML)
		{
			/*
			 * There is an existing connection, but we cannot use it and it executed
			 * DML. Any subsequent operation needs to be able to see the results of
			 * the DML command and thus cannot proceed if it cannot use the connection.
			 *
			 * Note that this is not meaningfully different from the previous case. We
			 * just produce a different error message based on whether DDL was or only
			 * DML was executed.
			 */
            。。。。。。
			ereport(ERROR,(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				   errmsg("cannot establish a new connection for "
						  "placement " UINT64_FORMAT
						  ", since DML has been executed on a connection that is in use",
							placement->placementId)));
		}
        //限制6
		else if (accessType == PLACEMENT_ACCESS_DDL)
		{
			/*
			 * There is an existing connection, but we cannot use it and we want to
			 * execute DDL. The operation on the existing connection might conflict
			 * with the DDL statement.
			 */
            。。。。。。
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot perform a parallel DDL command because multiple "
							"placements have been accessed over the same connection")));
		}
        //限制7
		else
		{
			/*
			 * The placement has a connection assigned to it, but it cannot be used,
			 * most likely because it has been claimed exclusively. Fortunately, it
			 * has only been used for reads and we're not performing a DDL command.
			 * We can therefore use a different connection for this placement.
			 */
            。。。。。。
		}
        。。。。。。
	}
	return chosenConnection;
}
```

在上面代码中，需要注意的是placementAccessList链表中存放的是该批次操作，内容为涉及到的placement和其对应的操作，比如placementId为1和2，都要进行SELECT和DML操作，总计4个操作，那么将为每个操作尝试分别获取可复用的连接，但是最终会根据上面的七个限制，选择出一个符合条件的连接赋值给chosenConnection，若没有符合条件的连接，则赋值为NULL。

其实上面的七个限制，可以进行合并，最终可归纳为以下几个分片缓存中的连接复用策略：

策略1：在一个事务中，若该placement在进行DDL操作，则只能够为其复用一个之前只进行单纯读操作的连接。

策略2：在一个事务中，若该placement在进行修改（DML或DDL）操作，则后续的所有涉及到该placement的操作，都必须复用该连接。

策略3：一旦满足策略1或策略2的条件，但是没法复用，则报错；无法复用的原因可能是：1、该连接已经被独占使用。2、该placement之前已经有多个read操作的连接分配在其上，但是当前又在进行DDL操作。3、为多个修改（DML或DDL）操作，分配了不同的连接。

### 3.3、分片缓存中查找连接FindOrCreatePlacementEntry

这个函数主要是从两个分片相关hash表，即ConnectionPlacementHash（分片位置连接管理hash表，见2.3节）和ColocatedPlacementsHash（亲和位置连接管理hash表，见2.4节）中查找是否存在可以复用的连接；

该函数的实现如下所示：

```c
/*
 * FindOrCreatePlacementEntry finds a placement entry in either the
 * placement->connection hash or the co-located placements->connection hash,
 * or adds a new entry if the placement has not yet been accessed in the
 * current transaction.
 */
static ConnectionPlacementHashEntry *
FindOrCreatePlacementEntry(ShardPlacement *placement)
{
	ConnectionPlacementHashKey key;
	ConnectionPlacementHashEntry *placementEntry = NULL;
	bool found = false;
	key.placementId = placement->placementId;
    //根据placementId作为key，从ConnectionPlacementHash中查找是否存在该key的可用连接
	placementEntry = hash_search(ConnectionPlacementHash, &key, HASH_ENTER, &found);
	if (!found)
	{   
        //如果没有在ConnectionPlacementHash中找到，则从ColocatedPlacementsHash中尝试寻找
		/* no connection has been chosen for this placement */
		。。。。。。
		if (placement->partitionMethod == DISTRIBUTE_BY_HASH ||
			placement->partitionMethod == DISTRIBUTE_BY_NONE)
		{
			ColocatedPlacementsHashKey key;
			ColocatedPlacementsHashEntry *colocatedEntry = NULL;
			strlcpy(key.nodeName, placement->nodeName, MAX_NODE_LENGTH);
			key.nodePort = placement->nodePort;
			key.colocationGroupId = placement->colocationGroupId;
			key.representativeValue = placement->representativeValue;

			/* look for a connection assigned to co-located placements */
            /*根据nodeName、nodePort、colocationGroupId和representativeValue联合作为key，从                 ColocatedPlacementsHash中查找是否存在该key的可用连接*/
			colocatedEntry = hash_search(ColocatedPlacementsHash, &key, HASH_ENTER,
										 &found);
			if (!found)
			{
				。。。。。。
			}
			/*
			 * Assign the connection reference for the set of co-located placements
			 * to the current placement.
			 */
			placementEntry->primaryConnection = colocatedEntry->primaryConnection;
			placementEntry->colocatedEntry = colocatedEntry;
		}
		else
		{
			void *conRef = MemoryContextAllocZero(TopTransactionContext,
												  sizeof(ConnectionReference));
			placementEntry->primaryConnection = (ConnectionReference *) conRef;
		}
	}
	/* record association with shard, for invalidation */
	AssociatePlacementWithShard(placementEntry, placement);
	return placementEntry;
}
```

从上面的代码可以看出，从分片缓存在查找连接的步骤和顺序是：

步骤1、先根据该Placement中的placementId作为key，从ConnectionPlacementHash表中查找是否存在该key的可用连接，若有，则尝试复用；否则若没有找到，就把该key插入到该hash表中，先占用一个位置，因为后面会为其分配或新建一个连接，然后跳至步骤2。

步骤2、根据该Placement中的nodeName、nodePort、colocationGroupId和representativeValue联合作为key，从其亲和关系ColocatedPlacementsHash表中查找是否存在该key的可用连接，若有则直接复用，否则若没有找到，就把该key插入到该hash表中，先占用一个位置，因为后面会为其分配或新建一个连接。

### 3.4、节点连接池中查找或新建连接StartNodeUserDatabaseConnection

若在前面两节中，没有从两个缓存分片连接的hash表中找到合适的连接，那么将会进入到第3.1节介绍的步骤2，即从节点连接池中查找或新建一个连接，此处在看下3.1节介绍的StartPlacementListConnection函数：

```c
ultiConnection *
StartPlacementListConnection(uint32 flags, List *placementAccessList,
							 const char *userName)
{
	。。。。。。
	//步骤1：从两个缓存分片连接的hash表中寻找合适的连接
	chosenConnection = FindPlacementListConnection(flags, placementAccessList, userName,
												   &placementEntryList);	
    //步骤2：为NULL，表明没有从两个缓存分片连接的hash表中找到合适的连接，则从节点连接池中查找
    if (chosenConnection == NULL)
	{  
		/* use the first placement from the list to extract nodename and nodeport */
		ShardPlacementAccess *placementAccess =
			(ShardPlacementAccess *) linitial(placementAccessList);
		ShardPlacement *placement = placementAccess->placement;
		char *nodeName = placement->nodeName;
		int nodePort = placement->nodePort;

		/*
		 * No suitable connection in the placement->connection mapping, get one from
		 * the node->connection pool.
		 */
		chosenConnection = StartNodeUserDatabaseConnection(flags, nodeName, nodePort,
														   userName, NULL);

		if (flags & CONNECTION_PER_PLACEMENT &&
			ConnectionAccessedDifferentPlacement(chosenConnection, placement))
		{  //关键点3：该连接之前分配给过其他的placement或亲和表中不同representative值的placement,则强制新建连接
			/*
			 * Cached connection accessed a non-co-located placement in the same
			 * table or co-location group, while the caller asked for a connection
			 * per placement. Open a new connection instead.
			 *
			 * We use this for situations in which we want to use a different
			 * connection for every placement, such as COPY. If we blindly returned
			 * a cached conection that already modified a different, non-co-located
			 * placement B in the same table or in a table with the same co-location
			 * ID as the current placement, then we'd no longer able to write to
			 * placement B later in the COPY.
			 */
			chosenConnection = StartNodeUserDatabaseConnection(flags |
															   FORCE_NEW_CONNECTION,
															   nodeName, nodePort,
															   userName, NULL);
		}
	}
```

在上面的代码中，需要注意三点：

关键点1、步骤2中调用的StartNodeUserDatabaseConnection函数，传入的参数除了flag外，也就只有nodeName、 nodePort、userName，而这都是和分片placement无关的，仅仅和节点worker相关。

关键点2、和节点相关的三个参数，就是取placementAccessList链表中第一个placement的内容，这是因为citus每次都是给一个task所涉及的placements来获取连接的（即使有些情况涉及多个placement，比如分布表和复制表join，但是这些placement必定都是同一个主机的），所以该链表中，涉及placement的节点中worker信息是一样的。

关键点3、如果从节点连接池中复用了一个连接，还得判断该连接之前是否分配给过其他的placement或亲和表中不同representative值的placement，若是，则需要重新强制新建一个连接

而对于从节点连接池在查找连接的函数StartNodeUserDatabaseConnection，关键部分代码如下所示：

```c
/*
 * StartNodeUserDatabaseConnection() initiates a connection to a remote node.
 *
 * If user or database are NULL, the current session's defaults are used. The
 * following flags influence connection establishment behaviour:
 * - SESSION_LIFESPAN - the connection should persist after transaction end
 * - FORCE_NEW_CONNECTION - a new connection is required
 *
 * The returned connection has only been initiated, not fully
 * established. That's useful to allow parallel connection establishment. If
 * that's not desired use the Get* variant.
 */
MultiConnection *
StartNodeUserDatabaseConnection(uint32 flags, const char *hostname, int32 port, const
								char *user, const char *database)
{
	ConnectionHashKey key;
	。。。。。。
	strlcpy(key.hostname, hostname, MAX_NODE_LENGTH);
	。。。。。。
	key.port = port;
    strlcpy(key.user, user, NAMEDATALEN);
    strlcpy(key.database, database, NAMEDATALEN);	
    。。。。。。
	/*
	 * Lookup relevant hash entry. We always enter. If only a cached
	 * connection is desired, and there's none, we'll simply leave the
	 * connection list empty.
	 */
    //步骤1、根据用户名、连接数据库、主机名和端口作为key，从ConnectionHash表中寻找涉及该节点的连接链表 
	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{   //如果没有找到，则把该key插入其中，并且new一个空的连接链表，后面会给其创建一个真实的连接
		entry->connections = MemoryContextAlloc(ConnectionContext,sizeof(dlist_head));
		dlist_init(entry->connections);
	}

	/* if desired, check whether there's a usable connection */
    //步骤2、从找出的节点的连接链表中，寻找是否存在可以复用的连接
	if (!(flags & FORCE_NEW_CONNECTION))
	{   //如果flag没有带FORCE_NEW_CONNECTION标志，则尝试从连接池中复用连接
		/* check connection cache for a connection that's not already in use */
		connection = FindAvailableConnection(entry->connections, flags);
		if (connection)
		{    //如果连接池中找到可以复用的连接，则直接返回
			if (flags & SESSION_LIFESPAN)
			{//如果要求选择的连接，在事务结束后也依然存在，则置sessionLifespan为true（表明为长连接）
				connection->sessionLifespan = true;
			}
			return connection;
		}
	}
	/*
	 * Either no caching desired, or no pre-established, non-claimed,
	 * connection present. Initiate connection establishment.
	 */
    //步骤3、若没有可复用的连接，则创建一个新的连接，注意此处仅仅是创建连接，而不会阻塞等待连接创建完成
	connection = StartConnectionEstablishment(&key);
    。。。。。。
	if (flags & SESSION_LIFESPAN)
	{//如果要求选择的连接，在事务结束后也依然存在，则置sessionLifespan为true（表明为长连接）
		connection->sessionLifespan = true;
	}
	return connection;
}
```

从上面代码所带注释，其实也可以看出从节点连接池中获取连接的步骤：

步骤1、根据nodeName、 nodePort、userName、database作为key，从节点连接池ConnectionHash中寻找是否存在涉及该节点的连接链表，若找到则直接调至步骤2，反之若没有找到，则把该key插入其中，并且new一个空的连接链表，后面会给其创建一个真实的连接；

步骤2、若flag没有带FORCE_NEW_CONNECTION标志，则从找出的节点连接链表中，寻找是否存在可以复用（即claimedExclusively为false）的连接，若找到，则直接返回，反之若没有找到，则跳至步骤3。

步骤3、若没有可复用的连接，则创建一个新的连接，注意此处仅仅是创建连接，而不会阻塞等待连接创建完成，只有等所有Placement都分配了连接后，才会统一去进行等待和判断连接的是否创建成功。

注意点：

1、flags带FORCE_NEW_CONNECTION标志位，则每次都会新建一个新的连接，而不会选择复用。

2、flags带SESSION_LIFESPAN标志位，则表明该连接，不管是新建还是复用的，都变为长连接，在当前事务结束后，该连接也不会被关闭和断开连接，除非当前session结束。

### 3.5、梳理和分配查找出的连接

因为前面的3.2节和3.4节找出了一个可用的连接chosenConnection，可需要被分配连接的placement可能有多个，而且对于每一个placement也可能涉及到多个操作，如何给这些操作分配连接，就是本节需要讨论的，而代码实现就是3.1节的步骤3，关键代码如下所示：

```c
MultiConnection *
StartPlacementListConnection(uint32 flags,List *placementAccessList,const char *userName)
{
	//步骤1：从两个分片缓存中查找是否存在可以复用的连接
	chosenConnection = FindPlacementListConnection(flags, placementAccessList, userName,
	        									   &placementEntryList);
    //步骤2：若步骤1没有找到，则从节点连接池中查找可复用连接，即使找不到也会新建一个连接
	if (chosenConnection == NULL)
	{
		。。。。。。
        chosenConnection = StartNodeUserDatabaseConnection(flags, nodeName, nodePort,
														   userName, NULL);
        。。。。。。
	}
	/*
	 * Now that a connection has been chosen, initialise or update the connection
	 * references for all placements.
	 */
    //步骤3:给每个placement的每个操作，遵循相关规则，各自分配可用连接
	forboth(placementAccessCell, placementAccessList,
			placementEntryCell, placementEntryList)
	{
		ShardPlacementAccess *placementAccess =
			(ShardPlacementAccess *) lfirst(placementAccessCell);
		ShardPlacementAccessType accessType = placementAccess->accessType;
		ConnectionPlacementHashEntry *placementEntry =
			(ConnectionPlacementHashEntry *) lfirst(placementEntryCell);
		ConnectionReference *placementConnection = placementEntry->primaryConnection;

		if (placementConnection->connection == chosenConnection)
		{   //若该placement的当前操作在步骤1已经分配的连接，和chosenConnection一样，则不用处理
			/* using the connection that was already assigned to the placement */
		}
		else if (placementConnection->connection == NULL)
		{   //若该placement的当前操作在步骤1没有被分配连接，则把chosenConnection分配给它
			/* placement does not have a connection assigned yet */
			placementConnection->connection = chosenConnection;
			。。。。。。
		}
		else
		{   //若该placement的当前操作在步骤1已经分配的连接，和chosenConnection不一样
			/* using a different connection than the one assigned to the placement */
			if (accessType != PLACEMENT_ACCESS_SELECT)
			{   //若该placement的当前操作是DDL或DML操作，则把chosenConnection分配给它
				/*
				 * We previously read from the placement, but now we're writing to
				 * it (if we had written to the placement, we would have either chosen
				 * the same connection, or errored out). Update the connection reference
				 * to point to the connection used for writing. We don't need to remember
				 * the existing connection since we won't be able to reuse it for
				 * accessing the placement. However, we do register that it exists in
				 * hasSecondaryConnections.
				 */
				placementConnection->connection = chosenConnection;
				。。。。。。
			}
			/*
			 * There are now multiple connections that read from the placement
			 * and DDL commands are forbidden.
			 */
            //走到此处，则可以知道有多个不同的连接，都在read该placement
			placementEntry->hasSecondaryConnections = true;
			if (placementEntry->colocatedEntry != NULL)
			{
				/* we also remember this for co-located placements */
				placementEntry->colocatedEntry->hasSecondaryConnections = true;
			}
		}
        。。。。。。
	}
    。。。。。。
	return chosenConnection;
}
```

在上面的代码片中，重点关注步骤3中的代码，先介绍下其中几个变量的含义：

1、***chosenConnection***，为步骤1和2中最终选择出的一个可用连接。

2、***placementConnection->connection***，保存当前placement的当前操作所分配的连接，若在步骤1从两个分片相关的缓存中，给当前placement的当前操作找到了可复用连接，则在进入步骤3时，所含值不为NULL。

3、***placementEntry->colocatedEntry***，保存当前placement所对应的亲和hash表中的值。

4、***accessType***，为当前循环中，当前placement所对应的操作，也就是将要执行的操作。

具体分配连接的规则是，对当前执行计划，依次遍历涉及到的各placement的每个操作，分别进入如下三个分支中的一个：

分支1、若该placement的当前操作在步骤1已经分配了连接，且该连接和chosenConnection一样，则不用处理。

分支2、若该placement的当前操作在步骤1没有被分配连接，则把chosenConnection分配给它。

分支3、若该placement的当前操作在步骤1已经分配了连接，但是该连接和chosenConnection不一样，若当前操作为SELECT，则继续复用之前的连接；反之当前操作为DML或DDL，则把chosenConnection分配给它。

注意点：

- 1、在分支3中，该placement必定是之前已经有一个连接在其上进行操作，并且一定是SELECT操作，因为若是DML或DDL操作，那么该事务后续涉及该placement的操作必定会复用此连接，也就是chosenConnection会选择该连接，从而进入分支1。

- 2、在分支3中，因为该placement之前已经有一个read操作的连接在其上，而当前操作选择的chosenConnection连接是另一个，为了标识这种一个事务中，一个placement涉及多个连接在其上进行操作（必定有一个read操作），则设置placementEntry->hasSecondaryConnections为true，同时在其对应的亲和关系中，也设置该标识，以便后续不会在该placement上进行DDL操作（参见3.2节限制1和限制2）。

## 四、单表的连接复用

在本节将着重讲解对于只涉及一个表的SQL语句中，带分片字段和不带分片字段之间，连接复用流程以及长短连接创建流程。

### 4.1、环境信息和测试语句

为了便于叙述，本文没有采用MX架构，而是简单的一个CN节点，两个worker节点的环境，这三个节点都在独立的主机上面，环境信息如下：

| 节点类型         | 主机名 | ip             | 端口 |
| ---------------- | ------ | -------------- | ---- |
| 客户端client     | client | 192.168.80.231 |      |
| Coordinate（CN） | cn     | 192.168.80.230 | 5432 |
| Worker           | pg1    | 192.168.80.220 | 5432 |
| Worker           | pg2    | 192.168.80.221 | 5432 |

创表（无副本）和填充数据语句为：

```sql
set citus.shard_count = 4;
set citus.shard_replication_factor = 1;
create table tbl1(id int,name varchar);
SELECT create_distributed_table('tbl1', 'id');

insert into tbl1 select t::int,t::varchar from generate_series(1,10) as t;
```

tbl1表的元数据分布情况

![1551261566261](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1551261566261.png?raw=true)

<center>图10、tbl1表的元数据分布<center\>
测试语句为：

```sql
begin; 
insert into tbl1(id,name) values(6,'11'); 
update tbl1 set name = '3' where name = '3'; 
rollback;
```

对上面四条测试语句进行初步分析：

- begin语句：该语句发送到CN后，直接在CN内部处理，并不用和worker交互，故也不会创建和worker间的连接，CN直接返回给客户端响应，表示该begin已经执行成功。


- insert语句：该语句因为指代了分片字段id=6，所以可以直接确定是在哪个worker的哪个shard上面，又因为该表没有副本，所以只需要获取一条连接即可。


- update语句：该语句不带分片字段，所以CN收到客户端请求后，需要往tbl1涉及到的四个shard上发送处理后的SQL，因而需要四个连接。


- rollback语句：因为改事务中，在前面的操作中一共涉及到四个分片，并且为其分配了四个连接，所以会复用这四个连接，往四个分片上同时发送rollback。


因为begin和rollback并不会改变CN和worker之间的连接数，所以本文不对其进行仔细分析，下面将着重分析insert语句和update语句，并且只关心和连接相关的部分，至于执行计划的生成以及执行器的选择等，将由组内其他人员进行分享。

### 4.2、insert语句（带分片字段）

因为是带分片字段的insert语句，所以会进入[1.4.1节](#1)描述的串行修改任务执行流程，并且从1.4.1.2节可以知道该操作调用StartPlacementListConnection函数获取连接时，传入的参数placementAccessList中只有一个成员，也就是说只需要给一个placement（Placemid=3）的DML操作分配连接。

又根据前面3.3节和3.4节中可以知道，citus给一个task分配连接的顺序是依次从ConnectionPlacementHash（分片位置连接管理hash表，简称：**分片连接池**）、ColocatedPlacementsHash（亲和位置连接管理hash表，简称：**亲和连接池**）、ConnectionHash（节点连接管理hash表，简称：**节点连接池**）中寻找是否存在符合要求的连接，若都没有找到，则新建一个连接。

另外，也从第二章可以知道，**节点连接池**中的数据在事务结束后，只会清理和断开连接标志sessionLifespan为false的连接，而其他连接会被保留；但**分片连接池**和**亲和连接池**中保存的连接在事务结束后都会被释放。所以此处在执行insert时，**分片连接池**和**亲和连接池**中为空的，假设节点连接池也为空，则为该操作分配连接的流程为：

![1552575875375](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552575875375.png?raw=true)

<center>图11、insert语句（带分片字段）连接分配流程<center\>

从上图可以看出，**节点连接池**中，根据key (host, port, user, database)找出来的实例是一个连接链表，存放该节点的系列连接，而另外两个连接池中，根据key找出来的是一个连接（若存在）。

因为三个连接池都为空，所以最终会新建一个连接（地址为0x2a35bf0的连接Connection），并且把该连接插入到**节点连接池**对应的连接链表中，同时之前**分片连接池**和**亲和连接池**插入的Entry中，让各自的连接指针指向该Connection。而该连接也是最终分配给insert任务的连接。

### 4.3、update语句（不带分片字段）

因为该update语句没有带分片字段，所以其会进入1.4.2节描述的并行修改操作执行流程，citus分布式执行计划会对表tbl1的所有四个分片，分别制定一个执行任务，然后每个执行任务都会调用StartPlacementListConnection函数获取连接，又因为该语句不仅涉及到DML操作，还涉及到顺序扫描，故传入的参数placementAccessList中存在两个成员，由于每个任务涉及一个分片，必定这两个成员的placementid是一样的，只是操作不一样，一个是DML，另一个是SELECT，整个执行计划如下所示：

![1552551480595](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552551480595.png?raw=true)

<center>图12、update语句（不带分片字段）执行计划<center\>

对于第一个task，涉及的分片表tbl1_102008，placementId=1，hostname=pg1，colocationid（亲和id）=1，representativeValue(即hash范围最小值shardminvalue,简称Hmin)=-2147483648，涉及两个操作（DML和SELECT，但是由于这两个操作涉及的是同一个placementId，而且是先DML，所以这两个操作分配连接流程是一样的），分配连接流程为：

![1552575908317](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552575908317.png?raw=true)

<center>图13、update语句（不带分片字段）task1获取连接流程<center\>

对于第二个task，涉及的分片表tbl1_102009，placementId=2，hostname=pg2，Hmin=-1073741824，涉及两个操作（DML和SELECT），分配连接流程为：

![1552575928332](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552575928332.png?raw=true)

<center>图14、update语句（不带分片字段）task2获取连接流程<center\>

对于第三个task，涉及的分片表tbl1_102010，placementId=3，hostname=pg1，Hmin=0，涉及两个操作（DML和SELECT），因为在4.2节执行insert操作时，已经为其分配了一个连接，地址为0x2a35bf0，故能够从分片连接池中找出该连接，然后task3复用该连接，其复用连接流程为：

![1552575950950](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552575950950.png?raw=true)

<center>图15、update语句（不带分片字段）task3获取连接流程<center\>

对于第四个task，涉及的分片表tbl1_102011，placementId=4，hostname=pg2，Hmin=1073741824，其流程和task1类似，所以不在详细介绍。

所以从上面分析可以看出，该update操作，涉及四个分片，分为了四个task，在给每个task分配连接时，一共只新建了三个连接，而另一个连接是直接复用之前insert操作创建的连接。同理，若在该update执行完后，在涉及到该tbl1的操作，只要事务没有结束，后面一般都不会在新建连接（除非某些场景强制新建连接），而都是直接复用，不能够复用的直接报错。

## 五、亲和表间的连接复用

在本节着重讲解对于涉及两个亲和表间连接复用的具体流程和限制，其中最关键的限制是亲和表间连接的复用只能够在一个事务中，因为事务结束后亲和连接hash表会被清空；所以本章节将分别从同一事务和不同事务两种场景来说明。

### 5.1、同一事物内亲和表间连接复用

#### 5.1.1、环境信息和测试语句

创表一个和第四章用到的tbl1具有亲和关系（同分片数、同副本数、分布键类型一样）的表tbl2：

```sql
set citus.shard_count = 4;
set citus.shard_replication_factor = 1;
create table tbl2(id int,name varchar);
SELECT create_distributed_table('tbl2', 'id');

insert into tbl2 select t::int,t::varchar from generate_series(1,10) as t;
```

tbl2表的元数据分布情况

![1552613184160](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552613184160.png?raw=true)

<center>图16、tbl2表的元数据分布<center\>

测试语句为：

```sql
begin; 
insert into tbl1(id,name) values(6,'11'); --最终插入到tbl1_102010分区
update tbl2 set name = '3' where id = 6;  --最终插入到tbl2_102014分区
rollback;
```

对上面四条测试语句进行初步分析：

- begin和rollback语句前面第四章已经分析过了。

- insert语句：该语句的执行流程可以参见4.2节。

- update语句：为了直观该update是带了分片字段的，且和insert操作所带分片字段一样（其实只要是最终会落到对应亲和分区即可，即tbl1_102010对应的tbl2_102014）。

因为insert语句的执行流程可以参见4.2节，所以接下来重点关注update操作。

#### 5.1.2、update语句（带分片字段）

因为是带分片字段的update语句，所以会进入[1.4.1节](#1)描述的串行修改任务执行流程，只会有一个task，涉及的分片表tbl2_102014，placementId=7，hostname=pg1，colocationid（亲和id）=1，Hmin=0，涉及两个操作（DML和SELECT，但是由于这两个操作涉及的是同一个placementId，而且是先DML，所以这两个操作分配连接流程是一样的），复用连接的流程为：

![1552617794017](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552617794017.png?raw=true)

<center>图17、update语句（带分片字段）获取连接流程<center\>

### 5.2、非事务内亲和表连接无法复用

环境信息依然和5.1.1节一样，唯一不同的是测试语句，采用如下测试语句：

```sql
insert into tbl1(id,name) values(6,'11'); --最终插入到tbl1_102010分区
update tbl2 set name = '3' where id = 6;  --最终插入到tbl2_102014分区
```

#### 5.2.1、单一事务insert语句

该语句分配连接的流程和4.2节一样，唯一不同的是该语句为单一事务执行语句，所以执行完成后，会进入事务结束流程，它会回收各连接池中的资源，省去连接获取阶段，其他流程如下所示：

![1552620619489](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552620619489.png?raw=true)

<center>图18、单一事务insert语句获取连接流程<center\>

#### 5.2.2、单一事务update语句

该语句的执行流程和前面的5.1.2节类似，唯一不同的是，它是从节点连接池中复用连接，具体的流程如下所示：

![1552622332173](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552622332173.png?raw=true)

<center>图19、单一事务update语句获取连接流程<center\>

## 六、同事务中多条DML操作存在的限制

在第三章时，介绍过citus为了保证DML操作的读写一致性和防止死锁，在连接复用时存在以下两个规则：

规则1：当一个事务中若有SQL对某个分片执行了DML操作，那么后续涉及到该分片的所有的SQL都必须复用此连接（参见3.2节）；

规则2：当一个DML操作同时涉及多个分片时，要求给每个分片分配一个独占连接（参见1.4.2节和1.6节），避免两个及其以上的分片共用同一个连接。

若在一个事务中，满足规则1的条件，但是又因为规则2，使得规则1没法实现，从而报错，比如如下SQL：

```sql
--tbl1和tbl2是亲和表
begin;
insert into tbl1(id,name) values(6,'11'),(8,'11'); -- pg1主机上的placementid= 1、3
update tbl2 set name = '3' where name = '3';       -- placementid= 5、6、7、8
```

在执行update语句时都会提示“**ERROR:  cannot establish a new connection for placement 7, since DML has been executed on a connection that is in use**”的错误消息。

其实可以看出，这三条测试语句和第四章介绍的测试用例很类似。唯一的不同是此处insert语句除了插入(6,'11')这个记录外，还另外多了一条(8,'11')，也正是因为多了这条，导致执行update语句时因为规则2没法复用连接，从而违背了规则1，简单来说就是：

- insert语句：因为(6,'11')和(8,'11')这两条记录，会分别落到pg1主机placementid为1和3的分片上，但是由于是insert into ...... values的方式，所以采用顺序执行方式，这样两个分片会共用一个连接（假设Conn1）。

- update语句：因为不带分片，所以会采用并行执行策略，涉及到 5（亲和分片为tbl1的1）、6、7（亲和分片为tbl1的3）、8共四个分片，此时需要给这四个分片分别分配连接；又因为前面的insert操作给placementid为1和3分配了连接Conn1，这样按照规则1，要求placementid为5和7也需要复用Conn1（因为互为亲和关系），但是这又和规则2相悖，故提示错误。

下面将着重分析下这两条语句进行连接分配的流程以及出错的原因：

### 6.1、insert语句（带多个value）

对应insert into ...... values语句，不管后面跟的value有多少个，citus分布式执行计划先会根据分片字段把待插入的value按照分片进行归类，然后看这些value涉及到多少个分片，就会将整个insert拆解为多个task，最终采用1.4.1节的串行修改任务执行方式，让其依次执行每个task；这可以从该insert的执行计划中看出来：

![1552814141068](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552814141068.png?raw=true)

<center>图20、insert语句（带多个value）执行计划<center\>

对于第一个task，涉及的分片表tbl1_102008，placementId=1，hostname=pg1，colocationid（亲和id）=1，Hmin=-2147483648，涉及一个DML操作，分配连接流程为：

![1552814808794](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552814808794.png?raw=true)

<center>图21、insert语句（带多个value）task1获取连接流程<center\>

对于第二个task，涉及的分片表tbl1_102010，placementId=3，hostname=pg1，Hmin=0，涉及一个DML操作，因为在前一个任务task1已经分配了一个连接，地址为0x2a35bf0，故能够从节点连接池中找出该连接，然后task2复用该连接，其复用连接流程为：

![1552871434172](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552871434172.png?raw=true)
<center>图22、insert语句（带多个value）task2获取连接流程<center\>

### 6.2、update语句

因为该update语句和4.3节使用的一模一样，只是表变了，所以其执行计划和执行流程在此不再解释，下面重点介绍下其执行计划中四个task执行中获取连接的相关流程：

对于第一个task，涉及的分片表tbl2_102012，placementId=5，hostname=pg1，colocationid（亲和id）=1，Hmin=-2147483648，涉及两个操作（DML和SELECT），分配连接流程为：

![1552874788019](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552874788019.png?raw=true)
<center>图23、update语句task1获取连接流程<center\>

需要注意，因为是并行执行任务，所以此处给task1任务涉及的分片（placementId=5）复用的连接0x2a35bf0，设置了独占连接标记，表明在当前执行计划中，该连接不能够被其他分片所使用。

对于第二个task，涉及的分片表tbl2_102013，placementId=6，hostname=pg2，Hmin=-1073741824，涉及两个操作（DML和SELECT），分配连接流程为：

![1552876416983](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552876416983.png?raw=true)
<center>图24、update语句task3获取连接流程<center\>

对于第三个task，涉及的分片表tbl2_102014，placementId=7，hostname=pg1，Hmin=0，涉及两个操作（DML和SELECT），因为在6.1节执行insert操作时，已经为其亲和分片分配了一个连接，地址为0x2a35bf0，故能够从亲和连接池中找出该连接，然后task3尝试复用该连接，但是该连接在task1时已经被标记为独占，所以无法复用，这和第六章开头描述的规则1相悖，因此报出错误，其复用连接出现异常的流程为：

![1552877839424](https://github.com/liupan126/flyingddb/blob/master/doc/citus%E8%BF%9E%E6%8E%A5%E7%9A%84%E5%88%9B%E5%BB%BA%E4%B8%8E%E5%A4%8D%E7%94%A8%E6%BA%90%E7%A0%81%E6%B5%85%E6%9E%90image/1552877839424.png?raw=true)
<center>图25、update语句task3获取连接流程<center\>

### 6.3、规避方式

从上面6.2节的流程可以看出，出错的根因就是在进行并行任务执行流程时，若选择出的复用连接在之前的子任务中已经被独占，并且那个子任务是在进行DML或DDL操作，则当前任务因为复用连接失败而出错；所以要规避该错误，有两种方法：

方法一：执行DML操作时，都带分片字段，这样就不会走并行执行流程；

方法二：通过SET命令，强制都进入顺序执行流程，而不进行并行执行流程，比如如下 所示SQL；

```sql
begin;
set citus.multi_shard_modify_mode = sequential;   --强制都走顺序执行流程
insert into tbl2(id,name) values(8,'11'),(6,'11');
update tbl2 set name = '3' where name = '3';
```


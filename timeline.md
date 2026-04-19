Mini-Ray 完整开发 Timeline（含 Task + Actor + Lineage 血统图）

适用于 Go 语言实现，多进程架构：Driver / Scheduler / Worker / GCS / Actor
一、核心概念一句话看懂

GCS：全局大脑，存储所有元数据、任务血统、对象位置、Actor 位置
Scheduler：交通警察，分发任务到 Worker / Actor
Worker：无状态执行单元，跑普通 Task
Actor：有状态远程对象，方法串行执行，有状态边
Lineage（血统图）
数据边：Task 之间的数据依赖
状态边：Actor 方法调用顺序链
作用：失败时根据血统重算任务 / 重放方法
二、整体架构交互流程

plaintext





Driver 提交任务/调用Actor
    ↓
Scheduler ←→ GCS（查询位置、依赖、状态）
    ↓
┌──────────┴──────────┐
Worker（执行Task）    Actor（执行方法，串行）
    ↓                    ↓
上报结果&对象位置 → GCS 更新血统&状态
三、分阶段开发 Timeline（每日 1~2 小时可完成）

第 1 周：基础骨架 —— 多进程 + gRPC + GCS 表结构

目标

搭建完整可启动的进程结构，实现基础通信
每日内容

Day1：项目结构、go.mod、公共结构体（Task/Actor/Object/Lineage）
Day2：gRPC Proto 定义（Scheduler/GCS/Worker/Actor 服务）
Day3：GCS 实现与核心表
TaskTable：任务 ID、状态、输入输出 ObjectID
ObjectTable：ObjectID → 节点地址
ActorTable：ActorID → 地址、方法队列、状态
LineageTable：任务 / 方法依赖图
Day4：Scheduler 服务实现（接收任务、查询 GCS）
Day5：Worker 服务实现（启动、注册 GCS、轮询拉取任务）
Day6：Actor 进程骨架（启动、注册 GCS、暴露调用接口）
Day7：联调：所有服务可独立启动、互相发现
第 2 周：Task 执行链路 —— 无状态远程函数

目标

实现完整远程 Task：提交 → 调度 → 执行 → 结果返回
核心内容

任务封装与传输
序列化：JSON / Protobuf
内容：TaskID、FuncName、Args、依赖 TaskID
函数注册机制
全局 FuncRegistry：函数名 → 函数体
Worker 本地必须包含所有可执行函数
Worker 执行逻辑
拉取 Task → 反序列化 → 反射执行 → 写对象存储
血统图（数据边）
GCS 记录：TaskB 依赖 TaskA
结果链路
Worker 上报结果 → GCS 更新状态 → Driver 可 Get
典型流程

plaintext





Driver.Submit(Add, 1, 2)
→ Scheduler
→ GCS 记录 Task & Lineage
→ Scheduler 分发至 Worker
→ Worker 执行、写 Object
→ GCS 更新 Task 状态
→ Driver.Get() 获取结果
第 3 周：Actor 实现 —— 有状态、串行、远程对象

目标

实现 Ray 核心 Actor 模型
核心内容

Actor 本质
独立进程
内部一个方法执行队列（FIFO）
同一 Actor 方法严格串行执行
GCS 中 Actor 信息
ActorID、Address、Status、PendingCalls
Actor 调用流程
plaintext





Driver.actor.Foo.Remote(arg)
→ Scheduler
→ GCS 查询 Actor 地址
→ 请求直接发往对应 Actor 进程
→ 进入 Actor 内部队列
→ 依次执行、修改内部状态
状态边（State Edge）
GCS 记录：ActorA.Method1 → Method2 → Method3
用于崩溃后重放方法链
Actor 内部结构

go


运行




type Actor struct {
    actorID string
    addr    string
    queue   chan MethodCall  // 方法队列
    state   interface{}      // 内部状态
}
第 4 周：Lineage 血统图完整实现

目标

同时支持 Task 数据依赖 + Actor 状态依赖
血统图结构

go


运行




type Lineage struct {
    TaskID      string   // 当前任务/方法ID
    Parents     []string // 依赖的上游任务
    Type        string   // "task" / "actor_method"
    ObjectID    string   // 输出数据
    ActorID     string   // 若是Actor方法则记录
    MethodIndex int      // 方法调用序号
}
两类边

数据边（Data Edge）
TaskA → TaskB（B 依赖 A 的输出）
状态边（State Edge）
Actor.Method1 → Method2 → Method3
第 5 周：Scheduler 调度策略

目标

区分 Task 与 Actor 的不同调度逻辑
Task 调度（无状态）
任意空闲 Worker 均可执行
数据本地性优先（GCS 查询对象位置）
Actor 调度（有状态）
必须路由到固定 Actor 进程
不负载均衡，不迁移
依赖调度
未满足依赖的任务进入等待
依赖完成后自动唤醒调度
第 6 周：容错机制 —— 基于 Lineage 重算

目标

实现 Mini-Ray 最接近真实 Ray 的核心能力
Worker 崩溃
Scheduler 发现任务超时 / 失联
GCS 查询 Lineage 找到依赖链
重新提交 Task 到其他 Worker
Actor 崩溃
重启新 Actor 进程
GCS 读取状态边（方法调用序列）
重放所有方法，恢复状态
对象丢失
无副本 → 根据血统重新生成
有副本 → 从其他节点拉取
四、GCS 核心存储表（必实现）

TaskTable
TaskID, Status, FuncName, Args, ParentIDs, OutputObjID
ObjectTable
ObjectID, NodeAddr, Size, RefCount
ActorTable
ActorID, Addr, Class, Status, MethodQueue, MethodIndex
LineageGraph
全局 DAG：Task & Actor 依赖关系
五、最小可运行 Demo 形态

go


运行




// Driver
func main() {
    // 无状态任务
    id1 := ray.Submit(add, 1, 2)
    res := ray.Get(id1)

    // 有状态 Actor
    actor := ray.StartActor(NewCounter)
    actor.Add.Remote(1)
    actor.Add.Remote(2)
    val := actor.Get.Remote()
}
六、关键结论（备忘）

不跨节点传输函数，只传函数名 + 参数
Worker 无状态、Actor 有状态、串行执行
Scheduler 只做调度，GCS 存储一切元信息
Lineage = 数据边 + 状态边 = 容错的唯一依据
分布式 = 多进程 + gRPC + 序列化 + 反射执行






cd /Users/hennessy/Documents/WorkSpace/mini-Ray && protoc -I proto --go_out=proto --go_opt=paths=source_relative --go-grpc_out=proto --go-grpc_opt=paths=source_relative proto/ray.proto 2>&1
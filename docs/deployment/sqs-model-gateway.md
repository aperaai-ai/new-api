# 全部模型调用经过 SQS：架构约定

状态：代码与 CDK 已接入，已进行本地模拟链路与真实 PostgreSQL 验证；尚未在目标 AWS 账号创建队列，也未完成真实 AWS/供应商端到端联调。

## 服务边界

所有模型供应商调用（普通 HTTP、SSE 流式、Tripo 提交与后台查询）必须通过 SQS 派发到 `apera-worker`。`apera-api` 负责认证、用户业务、价格与余额、请求入队、结果转发及结算，不直接连接供应商。登录、余额查询、管理后台和健康检查不入模型队列。

```text
客户端 → apera-api → PostgreSQL 任务记录与 outbox
                           ↓ outbox 发布器
                          SQS → apera-worker → 模型供应商
                                   ↓
                 PostgreSQL 执行状态 + S3 持久化结果
                                   ↓
                    Redis Streams 实时结果通知
                                   ↓
                         apera-api → 客户端
```

API 与 Worker 分别构建、部署和扩容。API 是余额与账本的唯一写入方，Worker 数据库角色只允许读取执行所需配置和更新执行状态，不允许改写用户余额。SQS 是任务派发通道，不能作为 HTTP 响应通道。

## 三种客户端体验

| 调用 | 客户端行为 | 结果返回 |
| --- | --- | --- |
| 普通模型请求 | 保持原有 HTTP 请求 | API 等待对应执行结果，然后返回供应商兼容响应 |
| SSE 流式聊天 | 保持原有 SSE 连接 | Worker 将响应头及有序数据块写入 Redis Stream，API 按游标读取并转发 |
| Tripo 等长任务 | 提交返回平台 task ID | Worker 主动短请求轮询供应商；客户端按 task ID 查询平台 |

“经过队列”不要求普通聊天也向客户端暴露异步任务接口。排队会增加首字节延迟，需要限制最长排队时间和单租户待执行任务数；超过容量时在接受前拒绝，避免无限积压。

WebSocket / Realtime 不能直接套用 HTTP/SSE 结果通道。适配完成前明确拒绝此类调用，不得偷偷改为 API 直连供应商。

## 发布可靠性与重复投递

1. API 生成平台 execution ID，验证客户端 Idempotency-Key；同一身份和 key 的相同请求复用同一执行，不同请求体返回冲突。
2. 请求体及凭证加密写入私有对象存储。SQS 消息只携带 execution ID，不携带密钥或大请求体。
3. API 先在数据库事务内创建持久化 QueueHold 并扣减钱包/Token 额度。随后另一个事务锁定 held 状态的冻结记录，原子创建 execution 和 outbox。若 API 在两步之间崩溃，后台在15分钟后释放没有任何执行的冻结记录；释放后禁止再关联新执行。创建执行与 outbox 不可拆成两个事务。
4. outbox 发布器发送 SQS 后标记 published。发送成功但数据库更新失败时允许重复发送。
5. Worker 使用数据库 queued → running 条件更新抢占；只有一个 Worker 获得执行权。SQS visibility timeout、心跳及数据库租约共同管理存活状态，但 SQS 去重并不等于供应商恰好执行一次。
6. Worker 先持久化响应与最终状态，再删除 SQS 消息。消息进入 DLQ 不代表模型没有执行，更不能直接触发退款。

执行状态与 SQL 抢占在 `model/queue_execution.go`，冻结/结算在 `model/queue_hold.go`，SQS/S3/Redis 运行时在 `pkg/modelqueue/runtime.go`。供应商协议转换和用量解析目前仍复用 API 内已有适配器，真正的供应商 HTTP 连接由 Worker 发起。

## Worker 故障与成本边界

| 状态 | 恢复处理 |
| --- | --- |
| queued 且未到截止时间 | SQS 重投后允许抢占执行 |
| queued 且已过截止时间 | 标记 expired；由 API 幂等释放冻结余额 |
| running 且租约有效 | 重复消息不能再次调用供应商 |
| running 且租约过期 | 标记 unknown，停止自动重发和自动退款，进入对账 |
| 已保存 Tripo upstream task ID | 继续查询同一任务，不重新创建 |
| 已保存成功或明确失败结果 | 重投只读取结果，不再次调用；API 幂等结算 |

数据库抢占不能消除“供应商已接受，但本地尚未保存响应时进程崩溃”的窗口。只有供应商承诺的幂等键、可恢复任务 ID 或账单查询才能自动消除此歧义；没有这些能力时保留冻结金额并进入人工/后台对账。不能把连接中断当作失败直接退款，也不能盲目切换渠道。

每次供应商调用前检查执行权和剩余截止时间；租约丢失时取消本地调用，但取消不证明供应商没有执行。

## 结果通道与结算

Redis Stream 按 execution ID 隔离，事件包含序号、响应头、数据块、结束或错误；使用独立连接池，防止阻塞读取占满认证缓存连接。持久化完整结果到加密 S3 对象，PostgreSQL 保存对象引用与状态，Redis 仅用于低延迟分发。客户端已经收到部分内容后 Redis 丢失时，API 等待完整持久化结果并从已发送字节偏移处继续，可能暂停，但不重复发送先前内容。Stream 保留与容量必须覆盖允许的响应大小；不能静默裁剪尚未读取的数据。

API 从持久化执行结果和已冻结价格结算，通过唯一账本键防止重复扣款/退款。Tripo 结果先写入 AsyncResultEvent，由 API 后台结算器完成幂等结算；Worker 无权修改余额。普通请求目前由存活 API 解析用量后幂等结算；API 在解析/结算前崩溃、断流或供应商结果有歧义时保留冻结记录，需按供应商账单人工对账，不会自动猜测实际 token 数。后台只自动释放确定未开始执行的过期冻结。流式中途断线不代表免费，也不自动创建新的执行。初版不承诺兼容接口断线后恢复 SSE；客户端重发不得自动重复消费同一计费请求。

## AWS 与验收条件

基础设施使用现有 CDK 模板新增 Standard SQS、DLQ、私有加密 S3、IAM 权限和 CloudWatch 告警。API 只允许发布任务，Worker 消费任务；两者仅获各自需要的对象路径权限。API 网络出口应限制为必要基础服务，以验证不存在供应商直连绕过。

合并完整功能前必须验证：

- 普通响应、SSE、Tripo 提交及轮询均由 Worker 发起，所有供应商适配器和 SDK 无直连旁路。
- 同一消息并发重复投递只触发一次抢占；outbox 发送后崩溃不会丢任务。
- Worker 在供应商调用前、调用中、结果持久化前后崩溃均有明确状态，歧义场景不会重发或误退款。
- API 重启、客户端断开、Redis 暂时不可用、SQS/DLQ 重投不会重复结算；对象存储有保留与清理策略。
- 有租户容量限制、执行并发上限、队列年龄与深度、首字节延迟、租约失效、unknown 数量和结算积压指标。
- 使用真实 AWS 测试队列与测试供应商完成端到端测试；通过模型状态单元测试不代表已完成上线验证。


## 部署与当前边界

- CDK 新建两个独立资源：ModelExecutionQueue（Standard、加密、长轮询20秒、visibility120秒、保留4天）和 ModelExecutionDLQ（重投8次转入、保留14天）。不是复用已有未知队列。CloudFormation 自动生成物理名称，URL/ARN 从 Stack Outputs 获得。
- 新建私有加密 S3 bucket；输入对象保留1天，结果保留14天。请求及 Redis 事件另用 CRYPTO_SECRET 做应用层 AES-GCM 加密。Redis 事件保留1小时。
- API 与 Worker 均注入 MODEL_QUEUE_ENABLED=true、MODEL_QUEUE_URL、MODEL_PAYLOAD_BUCKET、AWS_REGION、MODEL_WORKER_CONCURRENCY=4。Worker 的 Tripo 调度器也向队列发送 HTTP 操作，因而 Worker 同时具有发送/消费权限；API 只有发送权限。
- API 数据库账号 apera_app；Worker 独立账号/密码 apera_worker，只能访问执行/调度表和允许更新的任务列。迁移任务持有管理员连接，负责建表和授权。
- 两类 EC2 都使用 t3.medium，容器内存2GiB。每个 Worker 默认最多4个在途 HTTP 请求，两个 Worker 共8个；可用 MODEL_WORKER_CONCURRENCY 调整1–32，但扩大前必须按响应大小、持续时间、内存和 Redis 连接池压测。当前 Worker 副本数固定2，需要在 CDK 显式调整；SQS 本身不会自动增加 Worker。
- 最长排队60秒，单次 HTTP 执行总截止上限10分钟（调用方较短 deadline 优先）；请求体最多8MiB、累计响应最多32MiB。超过上限不走直连兜底。Tripo 提交/查询仍是短请求，不会保持连接等待3D生成完成。
- 目的地只允许公网 HTTPS:443；Worker 在 DNS 解析后屏蔽私网、回环、链路本地、IMDS 和常见 NAT64/CGNAT 地址。首版不支持私有 Ollama、通道代理、自签名 TLS 或 WebSocket（Realtime、Xunfei、VolcEngine WebSocket TTS）。
- 普通模型请求采用钱包计费，不支持订阅或 Playground 的无 API Token 计费路径。旧 Midjourney/视频/音乐任务提交入口在队列模式下明确返回501，避免旧计费与轮询路径接受后无人处理；Tripo 使用新的3D任务 API。
- SQS 重投会复用 execution ID。普通 HTTP 客户端发起的全新重试仍可能是新的收费请求；首版没有给所有同步兼容接口提供跨 HTTP 请求的 Idempotency-Key 重放协议。Tripo 保留现有客户端幂等接口。不可对未知执行自动重发。
- 两个服务的通知/登录业务保留在 API；账号通知 HTTP 不经过模型队列。
- SQL 执行记录/outbox 尚未自动归档清理，应在容量计划中计算增长；未知执行及未结算冻结不能按时间直接删除。

首次部署继续按 aws.md 的 bootstrap → 镜像 → migration → rollout 流程。已有环境升级先执行 `python3 deploy/scripts/prepare.py --image-tag GIT_SHA`，该步骤部署队列、存储、新密钥和迁移权限，同时固定当前 ECS service 的 task definition；随后运行 migrate.py，成功后执行不带 prepare context 的正常 CDK deploy。GitHub Actions 已串起这三个步骤。prepare.py 需要 HOSTED_ZONE_ID、HOSTED_ZONE_NAME、API_DOMAIN、API_COUNT 环境变量。

实际创建资源需要目标账号、Region、AWS 登录身份或仓库 production Environment 的 OIDC 配置；模板合成不会创建资源。创建后从 CloudFormation 输出 ModelQueueUrl / ModelDeadLetterQueueUrl / ModelPayloadBucket 核对资源，CloudWatch 检查队列年龄、可见/处理中消息及 DLQ。

## 人工对账入口

仅 Root 管理员可访问：

- GET /api/model-queue/holds：列出超过15分钟未完成结算的冻结记录，包含 RequestID 供日志关联，不返回提示词或凭证。
- POST /api/model-queue/holds/:id/reconcile：必须先在供应商侧核实；提交 action（settle/refund）、quota（平台整数额度，退款为0）、至少16字节的 reason，以及 confirmation=provider_verified。仍有 queued/running 执行时拒绝处理；账本、审计和额度更新在同一事务，重复结算不再扣款。

该入口是人工财务操作，不是“请求超时就退款”的自动策略。Tripo 的未知上游 task ID 仍使用原有 /api/async/tasks/:id/reconcile 流程。

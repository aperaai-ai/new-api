# Apera AWS 部署与模型执行队列

本部署沿用 new-api / QuantumNous 的现有网关、账户与控制台，新增独立模型执行层与全量 SQS 派发。**模板已提供，尚未在目标 AWS 账户创建资源或完成真实 Tripo 联调。**

## 目标架构

- 一个区域、两个可用区。默认 us-east-1，可通过 CDK_DEFAULT_REGION 改变。
- API：独立 ECS EC2 Capacity Provider / Auto Scaling Group；起步一个副本，最高十个。一个副本不保证无中断；服务器组覆盖两区，故障后可在另一区重建。
- Worker：另一个独立服务器组，两个副本，distinctInstance + AZ spread。API 与 Worker 不混用 EC2 实例。ECS Capacity Provider 管理底层 EC2 容量，发布期间可能临时多开机器。
- RDS PostgreSQL 16，t4g.medium，Multi-AZ **主实例加备用实例**，20GB gp3 起步，自动增长上限200GB；加密、删除保护、14天备份；删除 Stack 保留数据库。
- ElastiCache Redis 7.1，两个 cache.t4g.small 节点、主从及跨区自动故障转移；TLS、认证、静态加密、7天快照。
- 每区一个 NAT Gateway。API/Worker 在私有应用子网；数据在隔离子网；公网只开放 ALB HTTPS，HTTP 自动重定向。
- ECS on EC2，无 EKS/Helm 控制平面。CDK TypeScript 生成 CloudFormation；两个镜像分别用 Dockerfile.api / Dockerfile.worker 构建，linux/amd64。
- API 镜像内含两个现有前端主题，Worker 不含前端。前端和 API 共享域名。
- ACM 证书、Route 53 API 域名记录由模板创建。需要一个已授权的父域 Route 53 hosted zone；可通过可选 AperaDns Stack 创建，外部注册商的 NS 委派必须由域名所有者完成。
- CloudWatch 日志保留30天，含 ECS Container Insights、数据库/应用面板及 SNS 告警主题。告警主题必须订阅到实际值班渠道。仅创建主题不会有人收到通知。

## 仓库布局与启动职责

- 原 main.go：API + 控制台 + 协议转换、计费和结果转发；模型 HTTP 请求经 SQS 交给 Worker；AWS 设置 NODE_TYPE=slave，避免每个实例启动原有 master 专用维护任务。
- cmd/apera-worker：独立 HTTP 健康端口3000 + SQS 消费/模型 HTTP 执行 + Tripo 异步调度；不提供用户 API，不对公网开放。
- cmd/apera-migrate：一次性运行现有及新增表迁移；使用 RDS 管理账户，PostgreSQL advisory lock 串行化发布迁移；创建受限 apera_app 运行账户；首次用 Secrets Manager 随机密码建立 root 管理员，初始余额为0。不会覆盖已有管理员密码。
- API 使用 apera_app；Worker 使用独立凭证的 apera_worker，无钱包/Token/账本写权限。schema 权限升级仅由迁移任务执行。
- CDK 新建加密执行 SQS + DLQ、私有 S3，注入运行参数与按服务区分的 IAM 权限；队列、流式返回及财务边界详见 [SQS 方案](sqs-model-gateway.md)。
- /health/ready 检查数据库和 Redis；Worker 还检查执行循环心跳。
- API 正常处理 SIGTERM，最多等待110秒让在途请求结束。流式连接超过窗口仍可能中断，客户端需处理断线。
- 暂不支持旧 Midjourney/视频后台任务：它们原来的 master 专用轮询未搬到新 Worker。新增异步供应商走新执行层；普通/流式 LLM 的供应商 HTTP 由 Worker 执行；旧异步提交在队列模式下返回501。详见 [SQS 方案与限制](sqs-model-gateway.md)。

## 第一次部署

前置工具：Node22、npm、AWS CLI v2、Docker Buildx、Python3，及有相应权限的 AWS 登录身份。Go1.26.1 用于本地验证。不要把 AWS/Tripo 密钥写入 Git。

1. 确定区域、API域名、Route53 hosted zone。若没有 hosted zone：

   ```sh
   cd infra
   npm ci
   npx cdk deploy AperaDns -c createDnsZone=true --parameters ZoneName=example.com
   ```

   在注册商设置输出的 NS，并等待委派生效。若已有域名正在使用其他 DNS，先评估并迁移所需 DNS 记录，不要直接更换 NS。

2. 首次为账户/区域准备 CDK bootstrap。这是部署工具的基础资源，仍通过模板创建：

   ```sh
   npx cdk bootstrap aws://ACCOUNT_ID/us-east-1
   ```

3. 创建初始业务资源，应用副本为0，防止镜像/数据库尚未准备就循环启动：

   ```sh
   npx cdk deploy AperaProduction -c bootstrap=true \
     --parameters HostedZoneId=ZONE_ID \
     --parameters HostedZoneName=example.com \
     --parameters ApiDomain=api.example.com
   ```

   此时 RDS、Redis、NAT、ALB 及新建 SQS/S3 已进入计费生命周期。**bootstrap=true 只能用于新环境，绝不能对运行中的生产环境重复执行。**

4. 回到仓库根目录。使用 Stack 输出的 apiRepositoryUri / workerRepositoryUri，登录 ECR，分别构建推送同一个 Git SHA 标签。ECR 标签不可变；不要使用 latest。
5. 用新 API 镜像执行迁移，脚本通过 API Capacity Provider 临时调度一次性任务：

   ```sh
   python3 deploy/scripts/migrate.py --image-tag GIT_SHA
   ```

6. 回到 infra 目录执行正常 cdk deploy，带相同域名参数及 ImageTag=GIT_SHA、ApiCount=1。由此启动 API 和两个 Worker。
7. 从 InitialAdminSecretArn 读取首次 root 密码，通过 HTTPS 登录后修改并启用 MFA。密码只给运维人员，不能写日志/工单。
8. ProviderSecretArn 默认 enabled=false。设置下面的供应商配置，再滚动重启 API 和 Worker。Secrets Manager 更新不会自动刷新已运行容器的环境变量。
9. 验证登录、测试账户余额、创建/查询任务、幂等重放、故障恢复、退款及 SNS 通知。真实测试会产生供应商费用，按已批准的测试预算进行。

已有环境升级需先 prepare.py 更新资源/权限并固定正在运行的 task definition，迁移后再 rollout；不要跳过准备步骤。常规发布使用 .github/workflows/apera-deploy.yml，手动触发，限制 main 分支，production Environment 可设置审批。需要配置 GitHub OIDC 部署角色及 Environment variables：

- APERA_DEPLOY_ROLE_ARN（信任限制到此仓库的 production Environment）
- APERA_AWS_REGION
- APERA_HOSTED_ZONE_ID / APERA_HOSTED_ZONE_NAME
- APERA_API_DOMAIN
- APERA_API_COUNT，默认1

部署角色需要 assume CDK bootstrap 部署/查找/发布角色，以及读取 Stack 输出、推送 ECR、注册/运行/查询迁移任务和 PassRole（限定迁移 execution/task role）的权限。首次账户授权与 GitHub OIDC 信任须由 AWS 账户管理者配置，应用资源不需要控制台手动创建。

流水线先验证、构建、迁移，再滚动更新服务；ECS deployment circuit breaker 提供应用回滚。迁移必须保持前后版本兼容：先加字段/表，再切代码，最后单独清理。**应用回滚不自动撤销数据库迁移。**

## 异步供应商配置

ASYNC_PROVIDERS_JSON 存放在输出的 Secrets Manager secret，第一版只有 Tripo 适配器；新增供应商实现 pkg/asyncprovider.Provider（Prepare/Submit/Poll）并注册工厂。

以下 quota=500000 仅演示平台每任务固定收费1美元，不是 Tripo 官方报价；上线前按实际成本和售价确定。沿用平台默认500000 quota/$。不套用聊天 token 单价、动态表达式、订阅或 group 倍率；不同售价可使用不同模型 alias。参数固定标准档、texture=true、pbr=true，禁止传入未计价的任意参数。

```json
{
  "enabled": true,
  "accounts": {
    "tripo-production": {
      "provider": "tripo",
      "key": "SET_IN_SECRETS_MANAGER",
      "max_running": 10,
      "max_queued": 1000,
      "submit_rpm": 30,
      "poll_rpm": 60
    }
  },
  "models": {
    "tripo-standard": {
      "account": "tripo-production",
      "version": "v3.1-20260211",
      "quota": 500000,
      "groups": ["default"]
    }
  }
}
```

max_queued 是账户最大未结算任务总数（含排队和已提交任务），每用户另限制20个未结算任务。每个真实 Tripo 账户只配置一个 account entry；账户并发不能靠添加同账户 Key 增加。不同 Key 所属同账户无法从 Key 字符串自动识别，运维必须正确归组。提交/查询 RPM 独立限制，但当前按账户保守限流；生成账户池同时计算文生/图生 H 系列。多个 Worker **同一账户同一时刻由一个持有租约的 Worker 处理**；主要提供接替与跨账户并行，并非无限加 Worker 就提升一个账户吞吐。每轮最多一个提交、一批查询，2秒调度、最多四个账户并行；高吞吐优化需在实测后增加有界批次/独立调度。

价格、请求参数和 AES-GCM 加密凭证在接单时快照。旧任务一直使用原 Key；轮换前必须确保旧 Key 仍有效。CRYPTO_SECRET 必须跨实例一致并保留，不能随发布随机更换。删除 account entry 会停止其旧任务处理，需先排空或保留该账户。

API：

```sh
curl https://api.example.com/v1/3d/generations \
  -H "Authorization: Bearer $APERA_API_KEY" \
  -H "Idempotency-Key: CLIENT_GENERATED_UUID" \
  -H 'Content-Type: application/json' \
  -d '{"model":"tripo-standard","type":"text_to_model","prompt":"a wooden chair"}'

curl https://api.example.com/v1/3d/tasks/PLATFORM_TASK_ID \
  -H "Authorization: Bearer $APERA_API_KEY"
```

图生任务 type=image_to_model，input=公开HTTPS图片直链，不传prompt。第一版不接收其他用户的 file_token/task_id。客户端新操作生成新幂等键，同一次操作重试复用；相同键不同参数409。查询与列表按用户+调用Token隔离，已耗尽但未禁用/过期的Token仍可查询和重放；创建新任务需要余额。列表每页50条，before=上一页最后一个id。

鉴权、Token模型白名单、IP限制、显式分组限制在新接口执行；auto分组暂不支持。Redis入口限流每Token600 RPM，故障时停止新异步访问。创建成功返回202；重放返回200，Location指向任务地址。无Webhook；GET只查本地数据库。

## 可靠性、计费及人工核对

- SQL事务原子预占钱包及有限Token额度，并写hold账本。唯一幂等索引防止并发重复接单。
- 任务独立于原视频Task表，避免旧超时退款扫描影响。
- 账户数据库租约120秒，有界处理45秒；提交前再次校验租约。先持久化submitting再发请求。
- 返回upstream ID后保存；正常按15–20秒轮询，同凭证批量最多100个。查询失败/缺失延后60秒，其他未知状态120秒；不会退款。
- success且输出有效才结算；明确failed/cancelled释放预占。SQL CAS、账本唯一约束及事务保护重复结果，余额缓存失效有持久化重试。
- 网络超时、5xx、无有效ack、提交中崩溃 => reconciling，保留余额和账户占用，不自动重发。
- 仅明确未接受的限流响应可延后重试，最多五次后释放；本地排队超过30分钟且未被接受的任务释放。
- 整个异步功能要求BATCH_UPDATE_ENABLED=false，LOG_SQL_DSN为空，防止现有内存批写/异库日志破坏事务边界。已有同步聊天计费路径保持原样，本改动不宣称解决其所有既存并发计费问题。
- 本地缓存非事实来源。Redis清空会重置短期限流计数，账户执行租约和任务/账单仍由数据库保护。

Root 管理员可调用 GET /api/async/tasks/reconciling 查看待核对任务，最多100条。沿用控制台管理认证，需Authorization管理访问令牌或登录session及New-Api-User。

POST /api/async/tasks/:id/reconcile，仅允许**尚无上游ID**的reconciling任务：

- action=attach，upstream_task_id=已核实的ID，reason至少16字节。平台用任务原凭证查询验证，再关联恢复轮询；不会创建新任务，同一个上游任务不能重复关联。
- action=confirm_not_created，confirmation必须为 `I verified that no upstream task was created`，reason记录供应商核查证据/工单。该操作释放预占，**必须先向供应商确认未创建，不能仅因查询超时使用**。
- 两种操作都写入不可变的async_audits审计记录。

已知ID但上游长期失联/返回banned或expired的任务保留待核对，不提供无证据的一键退款。输出为供应商链接，可能过期；第一版不承诺永久托管模型文件，也不在API服务器中转大文件。

## 验证与上线门槛

- npm ci / npm run build / npm test / cdk synth（infra）；断言两NAT、独立服务器组、RDS主备、Redis主从、TLS和bootstrap不启动业务。
- Go新增测试覆盖参数/协议、未知提交、批量缺失、凭证绑定、幂等并发、重复结算/退款、额度不足回滚和租约接替。
- APERA_TEST_POSTGRES_DSN 可启用真实PostgreSQL并发测试，在全新临时schema中验证并清理。
- 本机无Docker/AWS CLI，镜像构建交给CI；尚无目标账户实测、Tripo真实账单验证或容量压测结果，不给出已验证RPS承诺。
- 全量Go测试已通过。已修复Claude附件类型转换和StreamStatus覆盖已有错误两处原有缺陷，并保留原测试及新增附件边界测试；部署流水线继续执行完整检查。
- 发布前执行重复请求、Worker提交阶段重启、数据库/Redis故障切换、恢复备份、队列满载与余额对账演练。
- 监控队列等待时间、reconciling数量、结算错误；单账户高负载优先提升上游额度及调度效率，不能只扩机器。

## 后续供应商接入

异步供应商复用任务、幂等、账本、账户租约及轮询，新增Provider适配器与配置校验。不同供应商确认拒绝、失败退款、计费终态的语义必须独立审核；不能假设所有failed都免费。同步/流式LLM继续走现有relay渠道体系。未来再统一控制台展示和渠道管理，不把任意模型请求都塞进Worker。

Worker 每30秒左右输出 async_queue_metrics 结构化日志，CloudWatch Metric Filter 提取 QueuedTasks、ReconcilingTasks、OldestQueuedSeconds。多 Worker 重复报告同一数据库快照，图表和告警使用 Maximum，避免重复相加。未知提交持续出现、排队超过5分钟或积压超过100条会触发SNS告警；阈值应随实际账户额度调整。

部署前还需要在目标AWS账户验证EC2/ECS调度、RDS证书、Redis故障转移、DNS验证及镜像启动；本地模板合成不能替代这些验证。

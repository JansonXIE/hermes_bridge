# Hermes × Claude Code 异步任务桥接服务

按 `hermes-claude-bridge.html` 方案实现 Phase 1（单机）和 Phase 2（接入 Hermes）。Hermes 负责决策，Claude Code 负责执行。任务后台运行，结束后自动唤起发起任务的 Hermes 会话，Hermes 不用占着一次模型调用等结果。

```
Hermes（claude-bridge 插件）
  │ claude_task_start / get / list / cancel        ▲ inject_message 唤起原会话
  ▼                                                │
API ──► bridge.db（SQLite WAL：任务、事件、Outbox）   Adapter（inbox 去重、会话队列）
          ▲                                        ▲
          │ 原子领取、租约                           │ HMAC 签名回调，至少一次
        Worker + Dispatcher ───────────────────────┘
          │ 参数数组启动，提示词走 stdin，输出写文件
          ▼
        claude -p（每个任务独立 worktree 和分支）
```

运行只需要 Python 3.11+、git 和已登录的 `claude` CLI，无第三方依赖。

## 一键部署

在要运行的服务器上，以使用者本人的账户执行（Hermes 也装在这台机器上时，插件会一并装好）：

```bash
deploy/setup.sh --restart-gateway    # 只部署服务；项目之后手动写进 bridge.toml
# 或者顺便加入仓库：deploy/setup.sh --repo /path/to/git-repo --repo /path/to/repo-workspace
```

手动加项目：生成的 `bridge.toml` 末尾有带注释的示例块。每个 git 仓库写一个 `[projects."ID"]` 块，**ID 里有 `.` 时必须加引号**。repo 工作区本身不是 git 仓库，需要给其中每个子仓库各写一个块。改完运行 `check`，再重启 `@api` 和 `@worker`。

`setup.sh` 依次完成：

1. 检查 Python 3.11+、git、claude。
2. 在 `~/hermes-bridge`（`--home` 可改）生成 `bridge.toml`、权限 600 的 `bridge.env`（三个随机密钥）和 `data/`。
3. 把每个 `--repo` 加成项目：
   - git 仓库：一个 worktree 项目，ID 为目录名，可用 `PATH=ID` 指定。
   - repo 工作区：每个子仓库一个项目，ID 为 `<工作区>-<子路径>`；工作区本身是只读的 inplace 项目。
4. 安装并启动 systemd 用户服务 `hermes-bridge@{api,worker,adapter}`，开启 linger，等健康检查通过。
5. 把插件装进 Hermes：写入 `.env` 的 4 个 `CLAUDE_BRIDGE_*` 变量，打开 `allow_gateway_injection`。改动前会备份。

可以重复执行：已有的密钥和配置保持不变，新的 `--repo` 会追加进去。同一服务名已指向别的 bridge 目录时，脚本在改动任何东西之前就退出。

默认权限只允许读改代码和只读 git 命令。`--full-access` 让 git 项目以 `bypassPermissions` 运行，可以执行任意命令（编译、`git fetch`、`git push`），**没有沙箱**。

**部署到另一台服务器：** 把本目录拷过去再运行 `setup.sh`。不需要拷贝 `bridge.toml`、`bridge.env`，它们会重新生成。

```bash
tar czf hermes-bridge-src.tgz --exclude=__pycache__ -C .. "$(basename "$PWD")"
scp hermes-bridge-src.tgz server:  &&  ssh server 'tar xzf hermes-bridge-src.tgz'
```

**之后再加仓库：** 用同一个 `--home` 再运行一次 `setup.sh --repo ...`。也可以手动执行，`bridge.env` 会自动读取：

```bash
python3.11 -m hermes_bridge -c ~/hermes-bridge/bridge.toml add-projects /path/to/repo [--full-access] [--dry-run]
python3.11 -m hermes_bridge -c ~/hermes-bridge/bridge.toml projects     # 列出项目 ID
systemctl --user restart hermes-bridge@api hermes-bridge@worker
```

## 手动运行（开发）

```bash
python3.11 -m hermes_bridge -c bridge.toml init      # 生成 bridge.toml 和 bridge.env，已存在则不动
python3.11 -m hermes_bridge -c bridge.toml check
python3.11 -m hermes_bridge -c bridge.toml all       # api、worker、adapter 同进程组运行
```

`bridge.example.toml` 是带注释的完整配置示例。

提交和观察任务（`set -a; . ./bridge.env; set +a` 后）：

```bash
A=http://127.0.0.1:8765; H="Authorization: Bearer $BRIDGE_API_TOKEN"
curl -X POST $A/v1/tasks -H "$H" -H 'Idempotency-Key: fix-login-1' -H 'Content-Type: application/json' -d '{
  "project_id": "backend-api", "hermes_session_id": "hs_123",
  "prompt": "修复登录接口并运行相关测试", "timeout_seconds": 1800, "depends_on": []
}'
curl "$A/v1/tasks?status=running,queued" -H "$H"     # 批量监控
curl "$A/v1/tasks/$ID" -H "$H"                       # 状态、最近动作、结果、变更、验收
curl -N "$A/v1/tasks/$ID/events" -H "$H"             # SSE；断线后用 Last-Event-ID 续传
curl -X POST "$A/v1/tasks/$ID/cancel" -H "$H"
curl "$A/v1/health" -H "$H"                          # Worker 心跳、各状态任务数、Outbox 积压
```

## 接入 Hermes

`setup.sh` 已包含这一步。只装插件时用 `deploy/install_plugin.sh`：装到 `$HERMES_HOME/plugins` 并启用，剩余步骤按提示手动完成。

Hermes 由此获得 `claude_task_start`、`claude_task_get`、`claude_task_list`、`claude_task_cancel` 四个工具。

唤起路径（已按 Hermes v0.20.1 的实际代码核对）：

| 会话类型 | 唤起方式 | 说明 |
|---|---|---|
| 消息网关（微信等） | 网关进程内的插件调用 `ctx.inject_message(session_key=...)` | 进入该会话原有的忙碌队列。需要配置 `plugins.entries.claude-bridge.allow_gateway_injection: true` |
| 交互式 CLI | CLI 进程内的插件调用 `ctx.inject_message()` | 该接口会打断运行中的回合，所以插件会等当前回合结束后再注入 |
| 已退出的 CLI | 可选的 `adapter.fallback_command` | 超过 `fallback_after_seconds` 仍无人领取时运行 `hermes chat --resume <id> -q ...` |

没有采用的入口：

- **Webhook 路由**：每次投递都开新的隔离会话，接不回原对话。
- **`hermes -z`**：不支持续接会话。

## 接口与语义

**状态：** `queued → starting → running → succeeded / failed`。取消：`queued → cancelled`，或 `starting / running → cancelling → cancelled`。超时：`timed_out`。租约失效：`recovering`，核对完进程后再决定去向。

| 字段 | 含义 |
|---|---|
| `status: succeeded` | CLI 正常结束：有最终 result 事件且 `is_error=false`，退出码为 0 |
| `acceptance.status` | `passed` / `failed` / `not_run`，由 Worker 运行项目的 `verify_command` 得出。**CLI 成功不等于验收通过** |
| `changes` | 任务分支、提交、变更文件；`uncommitted` 和 `snapshot_error` 表示有改动没能提交 |
| `error.code` | `no_result`、`process_lost`、`timeout`、`dependency_failed`、`dependency_merge_failed`、`continue_unavailable`，或 Claude 的 result subtype（如 `error_max_budget_usd`） |
| `result.permission_denials` | 被权限策略拒绝的工具调用。非交互运行时不会暂停等待确认，所以未实现 `input_required` |

**编排：**

- `depends_on`：依赖全部成功且验收未失败后才运行，并自动合并依赖分支；依赖失败时下游标记为 `dependency_failed`，不会启动。
- `kind: "integration"`：同项目串行运行，由 Claude 合并依赖分支并处理冲突，结束后 Worker 重新运行 `verify_command`。不会自动合入主分支。
- `continue_task_id`：用 `--resume` 向已结束任务的同一 Claude 会话和 worktree 发后续指令；同一会话不会有两个进程同时写入。
- `safe_to_retry`：默认 `false`。只有声明为 `true` 的任务，才会在进程丢失后自动重跑。
- `isolation = "inplace"`：项目不是单个 git 仓库（如 repo 工作区）时，只读任务直接在 `path` 中运行。只能允许 Read、Glob、Grep，始终禁止 Edit、Write、NotebookEdit、Bash，不能配置 `verify_command`，`gc` 不会回收该目录。显式设置 `allow_inplace_writes = true` 后取消上述工具限制，可配合 `bypassPermissions` 直接修改和编译该目录；此时同一项目的任务串行运行，不做快照提交，`changes` 为空。

**重启恢复：** Claude 在独立会话中运行，输出写文件，所以 Worker 崩溃或重启不影响它。新 Worker 用 pid、启动时间和 boot id 核对进程身份后接管，从保存的偏移量继续读日志，事件不丢不重。旧 Worker 的写入会被租约令牌拒绝。进程已消失时，若日志里有最终结果就按结果结束，否则标记 `process_lost`，由 Hermes 决定是否重跑。

**可靠通知：** 终态和 Outbox 在同一事务提交，指数退避重试。Adapter 先落库、按 `event_id` 去重，再应答。同一会话同时只有一个投递在途，短时间内连续完成的任务合并为一次唤起。整体是至少一次投递：若在注入后、确认前崩溃，会重复唤起一次，通知里的 `event_id` 可用于识别。

**运维命令：**

```bash
python3.11 -m hermes_bridge -c bridge.toml dead-letters            # 超过重试上限、待人工处理的通知
python3.11 -m hermes_bridge -c bridge.toml retry-dead <event_id>
python3.11 -m hermes_bridge -c bridge.toml gc --days 14 --dry-run  # 回收 worktree；保留任务记录和分支，跳过有未提交改动的目录
```

## 安全

- 所有接口都要鉴权；令牌只能访问自己的任务和被授权的项目；`hermes_session_id` 绑定首个使用者。
- 项目、回调地址、权限模式和允许的工具都只由服务端配置决定，请求无法指定。回调不跟随重定向。
- 默认 `permission_mode = "dontAsk"`。配置 `bypassPermissions` 必须同时显式设置 `allow_bypass_permissions = true`。
- 子进程环境变量走白名单，不会拿到桥接服务的令牌；嵌套 Claude 会话的变量会被清除。
- 事件和摘要写入前脱敏，不记录思考内容。原始输出保留在 `data/runs/<task>/attempt-N/`，目录权限 0700，**未脱敏**。
- 唤起消息只含 ID 和状态，不含 Worker 输出，避免提示注入进入 Hermes 的用户消息。`claude_task_get` 返回时会标注输出不可信。
- 桥接自身的提交（快照、合并）照常走仓库钩子，不使用 `--no-verify`。钩子拒绝时记入 `changes.snapshot_error`。默认提交信息符合本机 `commit-msg` 钩子的规范，可用项目配置 `commit_message` 覆盖。

Worker 与 Claude 以同一账户运行，隔离靠 worktree 和权限策略，**不是沙箱**。不可信任务请放进容器或换低权限账户运行。

## 部署

`deploy/hermes-bridge@.service` 是 systemd 用户单元模板，分别启用 `@api`、`@worker`、`@adapter` 三个实例。`setup.sh` 会替换其中的 `@CODE_DIR@`、`@BRIDGE_HOME@`、`@PYTHON@` 后安装。其中 `KillMode=process` 不能省略，否则重启 Worker 时 systemd 会把 Claude 一起杀掉。

修改 `bridge.toml` 或 `bridge.env` 后要重启服务：项目和 `[worker]` 的改动重启 `@api` 与 `@worker`，`bridge.env` 的改动三个都重启。改了 API 或 Adapter 令牌时，还要同步 Hermes 的 `.env` 并重启网关。重新运行 `setup.sh` 会自动完成这些步骤。

systemd 环境下要确认 Claude 的认证可用：`ANTHROPIC_*` 变量需要写进 `bridge.env`，或由 `~/.claude/settings.json` 提供。

默认值是配置示例，不是容量结论。实测中 Claude 的最小一次调用就报告了约 0.2–0.3 美元费用，`max_budget_usd` 设得太低会让任务以 `error_max_budget_usd` 失败。

## 测试

```bash
python3.11 -m unittest tests.test_acceptance tests.test_hermes_plugin tests.test_provision
```

测试启动真实的 api、worker、adapter 进程，用行为确定的假 Claude（事件格式取自真实 CLI）。`test_hermes_plugin` 使用本机已安装的 Hermes，未安装时跳过。

| 方案首版验收标准 | 对应测试 |
|---|---|
| 同时提交 5 个任务，最多运行 3 个 | `ConcurrencyAndIsolation` |
| 任务目录隔离 | `ConcurrencyAndIsolation` |
| 可查询进度并断线续传 | `ProgressAndResume` |
| 取消能停止子进程 | `Cancellation` |
| 回调失败后可重试且不重复唤起 | `ReliableNotification`、`SessionQueue` |
| 服务重启后不丢任务、不盲目重复执行 | `RestartRecovery` |
| 最终代码集成后重新测试 | `Integration` |

## 范围外

Phase 3（PostgreSQL、多机 Worker、对象存储）和标准 A2A 均未实现。当前接口是方案定义的自定义 HTTP 契约。

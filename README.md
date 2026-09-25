# deepspace — 深空测控指令裁决服务

纯后端 API（Go 1.23 + 标准库 `net/http` + PostgreSQL）。在中继并发领取上行指令、
失联者带着旧确认在租约过期后迟到的场景下，保证：

- **不重复结算**：一条指令至多一个终态；终态永不被覆盖。
- **前驱链有序放行**：后继只在整条前驱链确认送达后才可领取，绝不预发租约；前驱确认
  失败时，尚未送达的后继链原子转为只读 `blocked`（记录阻断来源，无租约、无伪造结算）。
- **不抢占新持有者结果**：旧代次令牌迟到时返回 `409`，状态不变。
- **重启后继续裁决**：指令、前驱闭包、租约代次、终态全部持久化在 PostgreSQL。

## 运行

需要 Docker Compose（内置 PostgreSQL）。

```bash
# 宿主机发布端口由 API_PORT 控制（默认 8080）
API_PORT=8080 docker compose up --build
```

`up` 会依次启动 `db`、`api` 与 **`verify` 一次性验收服务**。`verify` 跑完即退出，
退出码为 0 表示验收通过。单独重跑：

```bash
docker compose run --rm verify
```

### 本地开发

```bash
# 需要一个可达的 PostgreSQL
export DATABASE_URL="postgres://user:pass@localhost:5432/deepspace?sslmode=disable"
go run ./cmd/api            # 端口取 API_PORT，默认 8080

# 集成测试（自动创建隔离的临时数据库）
export DEEPSPACE_TEST_ADMIN_URL="postgres://postgres@localhost:5432/postgres?sslmode=disable"
go test -race ./...
```

服务首次连接数据库时会自动执行幂等迁移（带咨询锁，多实例同时启动也安全）。

## API 契约

所有请求/响应均为 `application/json`。错误响应使用**稳定信封**，禁止客户端依赖
自然语言文案：

```json
{ "error": { "code": "lease_expired", "message": "lease token is expired or superseded by a newer generation" } }
```

### `POST /commands` — 创建指令

按创建顺序写入（自增 ID 即顺序）。`payload` 必填且不能为 `null`，可以是任意 JSON 值。

请求：`{"payload": {"seq": 1, "target": "mars-relay-7"}}`

可选 `predecessor_id`（正整数）声明**唯一前驱**，用于“等上一条确认送达后才允许中继
领取后续动作”：

- 只能引用**已存在的较早编号**；缺省（或显式 `null`）即为无前驱的旧请求，行为不变。
- 前驱链尚未全部 `delivered` 时，该指令不可领取，也**不会预发租约**；前驱逐节送达
  后才自动开放领取。
- 前驱（直接前驱或任一祖先）确认 `failed` 时，尚未送达的整条后继链在同一事务内原子
  转为只读终态 `blocked` 并记录 `blocked_by`（阻断来源）；在失败之后才创建的后继出生
  即为 `blocked`。`blocked` 不产生租约、不产生结算。

请求示例：`{"payload": {"seq": 2}, "predecessor_id": 1}`

- `201` 返回指令对象（有前驱时含 `predecessor_id`，被阻断时含 `blocked_by`）
- `400 invalid_json`：请求体不是合法 JSON（或含多个 JSON 值）
- `422 missing_payload`：缺少 `payload` 或为 `null`
- `422 invalid_predecessor_id`：`predecessor_id` 不是正整数（字符串/浮点/布尔/0/负数）
- `422 predecessor_not_found`：引用了不存在或晚于本条的编号

### `POST /claims` — 原子领取最早可用指令

请求：`{"lease_duration_ms": 1000}`

- `lease_duration_ms` 必须是 **100–5000** 的整数，否则 `422 invalid_lease_duration`；
  字段缺失为 `422 missing_lease_duration`。
- 领取规则：状态为 `pending`、未持有有效（未过期）租约、且**整条前驱链都已
  `delivered`** 的指令中，取 **ID 最小**的一条。前驱未全部送达的指令一律等待，
  绝不预发租约；`delivered`/`failed`/`blocked` 终态指令不可领取。
- 并发安全：底层是对 `commands` 的 `SELECT ... ORDER BY id FOR UPDATE SKIP LOCKED`
  行锁扫描，前驱资格由传递闭包反连接在扫描中逐行求值，
  同一时刻并发的任意多个领取者中**恰有一个**能拿到同一条指令（等待中的后继不会被预发）。
- `200`：

  ```json
  {
    "command_id": 1,
    "generation": 1,
    "lease_token": "8d221c1e…(256bit 随机数的十六进制，不透明)",
    "lease_expires_at": "2026-09-18T00:00:01Z",
    "lease_duration_ms": 1000
  }
  ```

  每次领取都会推进该指令的 `lease_generation` 并签发**全新随机令牌**，
  新令牌必然不同于任何历史令牌（同时由数据库唯一约束兜底）。
- `204`：当前没有可领取的指令（无响应体）。

### `POST /commands/{id}/ack` — 凭租约确认终态

请求：`{"lease_token": "…", "result": "delivered"}`

- `result` 仅允许 `"delivered"` 或 `"failed"`，否则 `422 invalid_result`；
  令牌缺失为 `422 missing_lease_token`。
- 只有**未过期且属于当前代次**的令牌可以结算。裁决顺序（行锁内原子完成）：
  1. 指令已在终态（含 `blocked`）→ `409 already_settled`（重复结算/覆盖一律拒绝）；
  2. 令牌从未对该指令签发 → `409 invalid_lease_token`；
  3. 令牌属于旧代次 → `409 lease_expired`（失联者迟到）；
  4. 令牌已过期 → `409 lease_expired`。
- 成功 `200` 返回指令详情，`status` 变为 `delivered` 或 `failed`。
- `result="failed"` 在同一事务内把尚未送达的整条后继链原子置为只读 `blocked`，
  每条后继的 `blocked_by` 记录失败根编号；不写租约、不伪造结算。传播只作用于仍
  `pending` 的后继，已 `delivered` 的节点不受影响。
- 任意失败路径都**不会改变状态**。
- 未知编号：`404 unknown_command`（非数字 ID 同样按 404 处理）。

### `GET /commands/{id}` — 观察领取代次

`200` 返回指令详情：

```json
{
  "id": 1,
  "payload": {"seq": 1},
  "status": "failed",
  "lease_generation": 3,
  "current_lease_expires_at": "2026-09-18T00:00:03Z",
  "created_at": "…",
  "updated_at": "…",
  "leases": [
    { "generation": 1, "claimed_at": "…", "expires_at": "…" },
    { "generation": 2, "claimed_at": "…", "expires_at": "…" },
    { "generation": 3, "claimed_at": "…", "expires_at": "…" }
  ],
  "settlement": { "generation": 3, "result": "failed", "settled_at": "…" }
}
```

- 有前驱时返回 `predecessor_id`；被阻断时返回 `blocked_by`（阻断来源根编号）。
  无前驱/未阻断时这两个字段省略（旧指令响应形态不变）。
- `leases` 给出**每一代领取记录**，可观察代次推进；GET 永不返回令牌原文；
  从未领取（含 `blocked`）时为空数组 `[]`。
- 未结算时 `settlement` 为 `null`（`blocked` 同样为 `null`）。未知编号 `404 unknown_command`。

`GET /commands` 按创建顺序返回指令列表。`GET /healthz` 为存活探针。

### 状态码汇总

| 场景 | 状态码 | error.code |
| --- | --- | --- |
| 无可用指令可领取 | `204` | —（无体） |
| 未知指令编号 | `404` | `unknown_command` |
| 未知路由 / 方法不允许 | `404` / `405` | `route_not_found` / `method_not_allowed` |
| 请求体非法 JSON | `400` | `invalid_json` |
| 租期越界（<100 或 >5000ms）/ 缺失 | `422` | `invalid_lease_duration` / `missing_lease_duration` |
| 结果非法 / 令牌缺失 / 载荷缺失 | `422` | `invalid_result` / `missing_lease_token` / `missing_payload` |
| `predecessor_id` 非正整数 | `422` | `invalid_predecessor_id` |
| 前驱编号不存在或晚于本条 | `422` | `predecessor_not_found` |
| 旧代次或已过期令牌迟到 | `409` | `lease_expired` |
| 令牌从未对该指令签发 | `409` | `invalid_lease_token` |
| 重复结算 / 覆盖终态 / 确认 `blocked` 指令 | `409` | `already_settled` |

## 数据模型

迁移 SQL 内嵌于 `internal/store/migrations_sql/`，启动时自动应用：

- `commands`：指令本体、当前 `status`（`pending|delivered|failed|blocked`）、
  唯一 `predecessor_id`、阻断来源 `blocked_by`（与 `blocked` 状态由 CHECK 约束强制
  同生同灭）、当前 `lease_generation`、当前令牌与到期时间。
- `command_closure`：前驱关系的**传递闭包**，每条指令与其每个祖先各一行
  （`(command_id, ancestor_id)` 主键）。领取资格因此是普通反连接
  （“不存在状态非 delivered 的祖先”），可在 `commands` 的 `FOR UPDATE SKIP LOCKED`
  行锁扫描中逐行求值——避免“递归 CTE 先物化候选再加锁”在高并发下让两个领取者
  同时选中同一行的锁洞。
- `leases`：每一代租约（`(command_id, generation)` 唯一，`lease_token` 全局唯一），
  构成可观察的领取历史。`blocked` 指令从不写入租约。
- `settlements`：以 `command_id` 为主键——即使应用层存在疏漏，数据库层面也**物理禁止
  插入第二条终态**，是“不重复结算”的最后防线。`blocked` 不产生结算行（没有伪造结算）。

领取在单事务内完成（行锁扫描 + 闭包资格 + 代次推进 + 租约写入），确认在单事务内完成
（行锁 + 终态裁决 + `settlements` 插入 + 状态翻转 + 后继链原子转 `blocked`），
创建带前驱的指令也在单事务内完成（逐行锁定整条前驱链 + 状态裁决 + 闭包写入）。
因此创建与前驱结算交错时由数据库行锁裁决，任何崩溃/重启都不会留下“前驱已失败、
后继却仍可领取”，或半完成的裁决。

## 验收交错（`cmd/verify`）

`verify` 是一次性服务，断言完整生命周期：

1. 输入校验契约（422/404/400/204 与稳定错误码）；
2. **前驱链契约**：前驱未送达的后继不被预发租约、根送达后逐节解锁、链上 `failed`
   原子传播为后继 `blocked`（含阻断来源、无租约无结算）、失败后新建后继出生即
   `blocked`、无前驱旧指令照常领取；
3. 创建唯一指令，**16 个请求并发抢领**，断言恰有 1 个 `200`、其余全为 `204`；
4. 等待第一代租约到期后重新领取，断言代次推进、新令牌不同；
5. 旧令牌迟到确认 → `409 lease_expired` 且状态仍为 `pending`；
6. 再等第二代到期、领取第三代，用第二代旧令牌逆序确认 → `409`，状态不变；
7. 当前持有者置 `failed`；旧/新/持有者令牌再次确认全部 `409`，终态不被覆盖；
8. 直连 PostgreSQL 带外核对：`settlements` 恰 1 行、`leases` 恰 3 代、状态唯一；
9. **重启 API 进程**（同一数据库、新进程重新迁移连接），核对终态 `failed/gen=3`
   保持不变，且重启后重复确认仍为 `409`、终态指令仍不可领取。

退出码 0 即全部通过。

## 项目结构

```
cmd/api/main.go                 API 入口（迁移、连接重试、优雅关停）
cmd/verify/main.go              一次性验收服务
internal/httpapi/               路由、处理器、稳定 JSON 错误信封
internal/store/                 PostgreSQL 持久化与租约裁决
internal/store/migrations_sql/  内嵌迁移 SQL
internal/testdb/                测试用临时数据库辅助
```

-- 0001_init.sql
-- 深空测控指令裁决核心表结构。
--
-- 设计要点：
--   * commands.lease_generation 记录当前租约代次：每次被领取 +1；
--   * leases 表保存每一代租约（含令牌原文与到期时间），代次在命令范围内递增；
--   * lease_token 全局唯一：新令牌必然不同于任何旧令牌；
--   * settlements 以 command_id 为主键，从数据库层面禁止同一指令重复结算。

CREATE TABLE IF NOT EXISTS commands (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payload           JSONB        NOT NULL,
    status            TEXT         NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'delivered', 'failed')),
    lease_generation  BIGINT       NOT NULL DEFAULT 0,
    lease_token       TEXT,
    lease_expires_at  TIMESTAMPTZ,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 领取查询的固定路径：最早可用（未结算且未持有有效租约）指令。
CREATE INDEX IF NOT EXISTS commands_claim_idx
    ON commands (id)
    WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS leases (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    command_id   BIGINT       NOT NULL REFERENCES commands(id),
    generation   BIGINT       NOT NULL,
    lease_token  TEXT         NOT NULL,
    expires_at   TIMESTAMPTZ  NOT NULL,
    claimed_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (command_id, generation),
    UNIQUE (lease_token)
);

CREATE TABLE IF NOT EXISTS settlements (
    command_id          BIGINT      PRIMARY KEY REFERENCES commands(id),
    generation          BIGINT      NOT NULL,
    lease_token         TEXT        NOT NULL,
    result              TEXT        NOT NULL CHECK (result IN ('delivered', 'failed')),
    settled_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

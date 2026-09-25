-- 0002_chains.sql
-- 前驱链与 blocked 终态。
--
-- 设计要点：
--   * commands.predecessor_id 指向唯一前驱（自引用外键）；NULL 表示无前驱的旧指令，
--     旧数据无需回填，行为与过去完全一致；
--   * 只能引用已存在的较早编号：由应用层在创建事务内校验（自增 ID 天然晚于前驱），
--     外键负责兜底引用完整性；
--   * commands.blocked_by 记录阻断来源（确认失败的那条根指令）；
--   * status 增加只读终态 'blocked'：前驱确认失败后，尚未送达的后继链原子转入；
--   * blocked 与 blocked_by 必须同时出现/缺失，由数据库约束保证状态契约一致；
--   * blocked 指令永不写入 settlements/leases：它不产生租约，也不产生伪造结算；
--   * command_closure 是前驱关系的传递闭包：创建指令 c（前驱 p）时写入
--     (c, p) 以及 p 的全部 (p, a)，即 c 与其每一个祖先各一行。
--     领取资格因此是一个普通反连接（“不存在状态不为 delivered 的祖先”），
--     可在 commands 的 FOR UPDATE SKIP LOCKED 扫描中逐行求值，
--     避免“递归 CTE 先物化候选、再加行锁”在高并发下放过同一行的锁洞。

ALTER TABLE commands
    ADD COLUMN IF NOT EXISTS predecessor_id BIGINT NULL REFERENCES commands(id),
    ADD COLUMN IF NOT EXISTS blocked_by     BIGINT NULL;

-- 0001 内联 CHECK 的自动名是 commands_status_check：先摘除旧约束，
-- 再以显式命名重建为四态契约。重跑迁移时两处均幂等。
ALTER TABLE commands DROP CONSTRAINT IF EXISTS commands_status_check;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'commands_status_chk') THEN
        ALTER TABLE commands
            ADD CONSTRAINT commands_status_chk
            CHECK (status IN ('pending', 'delivered', 'failed', 'blocked'));
    END IF;
END$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'commands_blocked_source_chk') THEN
        ALTER TABLE commands
            ADD CONSTRAINT commands_blocked_source_chk
            CHECK ((status = 'blocked') = (blocked_by IS NOT NULL));
    END IF;
END$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'commands_blocked_by_fkey') THEN
        ALTER TABLE commands
            ADD CONSTRAINT commands_blocked_by_fkey
            FOREIGN KEY (blocked_by) REFERENCES commands(id);
    END IF;
END$$;

-- 直接前驱边（创建链、失败传播之外的排查/级联使用）。
CREATE INDEX IF NOT EXISTS commands_predecessor_idx
    ON commands (predecessor_id)
    WHERE predecessor_id IS NOT NULL;

-- 领取候选仍沿用 0001 的部分索引 commands_claim_idx (id) WHERE status='pending'：
-- 行锁扫描沿它按 id 行走，前驱闭包资格由存储层事务内的反连接逐行追加判定。

-- 传递闭包：(command_id, ancestor_id) 表示 ancestor_id 是 command_id 的任一前驱祖先。
-- 复合主键物理禁止重复边；两列分别外键到 commands，随指令删除级联。
CREATE TABLE IF NOT EXISTS command_closure (
    command_id  BIGINT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
    ancestor_id BIGINT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
    PRIMARY KEY (command_id, ancestor_id)
);

-- 反连接沿“祖先”一侧定位某指令的全部前驱：
-- 仅保留未 delivered 的祖先，能命中即代表该指令尚不可领取。
CREATE INDEX IF NOT EXISTS command_closure_ancestor_idx
    ON command_closure (command_id, ancestor_id);

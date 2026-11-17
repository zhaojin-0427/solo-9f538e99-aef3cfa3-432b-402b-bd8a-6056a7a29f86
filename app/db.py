"""SQLite 访问层：建表、连接管理、常量。

所有需要原子性的发布动作都在单个写事务中完成，并配合部分唯一索引保证：
- 同一输入版本最多一份可下载的已发布清单；
- 同一图像的侧别登记全局唯一（永久固定，不被后续分配改写）；
- 同一输入版本最多一个未作废的活动作业。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

MAX_IMAGES = 40
MAX_CLASSES = 4
VALID = "val"
TRAIN = "train"
SIDES = (TRAIN, VALID)


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS input_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    status TEXT NOT NULL DEFAULT 'active',          -- active | superseded
    image_csv TEXT NOT NULL,                        -- 原始 CSV 全文，留档
    dup_csv TEXT NOT NULL,
    image_count INTEGER NOT NULL,
    class_names TEXT NOT NULL,                      -- 逗号分隔，固定 4 类
    quotas TEXT NOT NULL,                           -- JSON: {类别: 验证张数}
    note TEXT
);

CREATE TABLE IF NOT EXISTS images (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL REFERENCES input_versions(id),
    image_no TEXT NOT NULL,
    patient_no TEXT NOT NULL,
    class_name TEXT NOT NULL,
    UNIQUE(version_id, image_no)
);

CREATE TABLE IF NOT EXISTS dup_edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL REFERENCES input_versions(id),
    image_a TEXT NOT NULL,
    image_b TEXT NOT NULL
);

-- 侧别登记：永久记录，图像号全局唯一。模型一旦在某侧“见过”，永不更改。
CREATE TABLE IF NOT EXISTS side_registry (
    image_no TEXT PRIMARY KEY,
    side TEXT NOT NULL CHECK (side IN ('train', 'val')),
    first_manifest_id INTEGER,
    locked_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS manifests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL REFERENCES input_versions(id),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    status TEXT NOT NULL CHECK (status IN ('published', 'failed')),
    reason TEXT,
    -- 历史身份还原判定该清单不可再供数（原编号、原始明细与字段全部保留）
    quarantined INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS manifest_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id INTEGER NOT NULL REFERENCES manifests(id),
    image_no TEXT NOT NULL,
    patient_no TEXT NOT NULL,
    class_name TEXT NOT NULL,
    component_id INTEGER NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('train', 'val')),
    UNIQUE(manifest_id, image_no)
);

CREATE TABLE IF NOT EXISTS partition_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL REFERENCES input_versions(id),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    finished_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    fail_stage TEXT,                               -- validation | conflict | quota | stale | restart
    reason TEXT,
    detail TEXT,                                   -- 结构化诊断 JSON
    manifest_id INTEGER,
    -- 1 表示该作业绑定的清单因历史身份还原而停供，作业不再占用“活动作业”唯一位，
    -- 其状态与原因仍完整保留可查。
    voided INTEGER NOT NULL DEFAULT 0
);

-- 同一输入版本至多一份可下载的已发布清单（失败/停供记录可多条，用于历史/原因查看）。
-- 受历史身份还原影响而停供的清单（quarantined=1）保留 status='published' 与原始明细，
-- 但不占用该唯一位，允许按真实身份重新划分并发布新的干净清单。索引在 init_db 中
-- 于列升级之后创建（旧库先 ALTER 再建索引）。

-- 已执行的数据升级（保证旧数据修复只跑一次；失败回滚时不会写入）。
CREATE TABLE IF NOT EXISTS schema_migrations (
    name TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 旧登记键留档：升级前的 side_registry 原值（含错误身份），仅供审计。
CREATE TABLE IF NOT EXISTS legacy_registry_archive (
    image_no TEXT PRIMARY KEY,
    side TEXT NOT NULL,
    first_manifest_id INTEGER,
    locked_at TEXT,
    archived_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 原发布明细的身份还原证据，每条旧明细一行；清单/明细原编号保持不变。
CREATE TABLE IF NOT EXISTS legacy_identity_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id INTEGER NOT NULL,
    version_id INTEGER NOT NULL,
    entry_id INTEGER NOT NULL,
    legacy_image_no TEXT NOT NULL,
    legacy_patient_no TEXT NOT NULL,
    legacy_class_name TEXT NOT NULL,
    resolved_image_no TEXT,
    candidates TEXT NOT NULL,                 -- JSON 数组，歧义时的全部候选
    resolution TEXT NOT NULL                  -- resolved | ambiguous | unresolved
        CHECK (resolution IN ('resolved', 'ambiguous', 'unresolved')),
    side TEXT NOT NULL CHECK (side IN ('train', 'val')),
    note TEXT,
    UNIQUE(entry_id)
);

-- 真实身份在历史证据中曾出现在两侧：无唯一历史侧别，涉及即关联冲突。
CREATE TABLE IF NOT EXISTS identity_conflicts (
    image_no TEXT PRIMARY KEY,
    sides TEXT NOT NULL,                      -- JSON 数组
    reason TEXT NOT NULL,
    evidence TEXT NOT NULL,                   -- JSON：清单/明细/证据编号
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 受影响旧清单：保留原编号与原始明细，仅停供下载，原因与证据可查。
CREATE TABLE IF NOT EXISTS manifest_quarantine (
    manifest_id INTEGER PRIMARY KEY REFERENCES manifests(id),
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,                     -- JSON：证据编号、相关图像
    quarantined_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 同一输入版本至多一个未作废的“进行中/成功”划分作业（数据库级硬约束，跨线程/进程有效）。
-- 失败作业与因历史身份还原停供而作废的作业不占该约束位，允许重新发起划分。
CREATE UNIQUE INDEX IF NOT EXISTS uq_job_active
    ON partition_jobs(version_id)
    WHERE status IN ('queued', 'running', 'succeeded') AND voided = 0;
"""


def get_db(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column in cols:
        return
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
    except sqlite3.OperationalError as exc:
        # 多进程同时启动时另一进程可能已加列；确认已存在即可继续。
        cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in cols:
            raise exc


def init_db(db_path: str | Path) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = get_db(db_path)
    try:
        conn.executescript(SCHEMA)
        # 旧库就地升级：manifests 增加隔离标志（不重建表，原编号/明细原样保留）。
        _ensure_column(conn, "manifests", "quarantined", "INTEGER NOT NULL DEFAULT 0")
        # 旧库就地升级：作业增加作废标志（停供清单对应作业不再占用活动唯一位）。
        _ensure_column(conn, "partition_jobs", "voided", "INTEGER NOT NULL DEFAULT 0")
        # 唯一索引只覆盖“可下载的干净清单”；停供清单不占位，允许重划分出新清单。
        conn.execute("DROP INDEX IF EXISTS uq_manifest_published")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_manifest_published_clean "
            "ON manifests(version_id) WHERE status='published' AND quarantined=0"
        )
        conn.execute("DROP INDEX IF EXISTS uq_job_active")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_job_active "
            "ON partition_jobs(version_id) "
            "WHERE status IN ('queued','running','succeeded') AND voided=0"
        )
        # 旧数据升级：失败整体回滚且不写迁移标记，下次启动会重试；
        # 成功则反复重启不会再执行（不会重复改写任何登记）。
        from .migration import migrate_legacy

        migrate_legacy(conn)
        # 重启恢复：进程内线程池不持久，残留 queued/running 任务一律判失败，
        # 绝不允许“半次分配”——它们从未写入 manifest/registry，直接收尾。
        conn.execute(
            "UPDATE partition_jobs SET status='failed', fail_stage='restart', "
            "finished_at=datetime('now'), "
            "reason='服务重启，该任务未完成，未产生任何分配，请重新发起划分' "
            "WHERE status IN ('queued','running')"
        )
    finally:
        conn.close()

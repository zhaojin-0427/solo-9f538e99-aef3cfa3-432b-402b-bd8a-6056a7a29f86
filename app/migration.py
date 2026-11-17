"""旧数据升级：修复“按表头识别列序”前的错误身份登记。

背景
----
旧解析器虽然能识别表头，却始终按文件的前三列取 ``(图像号, 患者号, 类别)``。
当上传的 CSV 表头列序不是该顺序（典型：``患者号,图像号,类别``）时：

* ``images`` / ``manifest_entries`` 把患者号当成图像号；
* ``side_registry`` 的键是错误身份（患者号），并不对应真实图像；
* 按正常列序重传同一批图像时，真实图像查不到历史登记，可能被改到另一侧。

本模块在启动时（每个数据库一次）执行原子升级：

1. 逐版本用**当前**解析器重新解析原始 CSV 全文，与旧 ``images`` 记录比对，
   识别受影响的旧版本；
2. 用原始 CSV 的已识别表头/行序，把每条**原发布明细**（manifest_entries，保持原编号）
   还原到真实图像身份；无法唯一对应时保留候选与证据，绝不猜一侧；
3. 汇总每张真实图像在全部历史发布中的侧别：
   - 仅一侧 → 重建为永久 ``side_registry`` 登记；
   - 曾出现在两侧 → 写入 ``identity_conflicts``，不进入登记表；
4. 受影响清单写入 ``manifest_quarantine``（保留原清单编号与明细，仅停供下载）；
5. 旧登记键完整留档到 ``legacy_registry_archive``，旧 ``images`` 派生表按真实解析修复，
   原始 ``input_versions.image_csv`` 与 ``manifest_entries`` 一律不改。

整个过程在单个 ``BEGIN IMMEDIATE`` 事务内完成，失败整体回滚且不写迁移标记，
不会留下半次修复；迁移标记保证反复重启只执行一次。
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field

from .parser import InputError, parse_dups_csv, parse_images_csv

MIGRATION_NAME = "legacy_identity_v1"


@dataclass
class VersionMapping:
    version_id: int
    corrupt: bool = False
    parse_error: bool = False
    # 按原始 CSV 重新解析出的真实 (图像号, 患者号, 类别)，保持文件行序
    true_rows: list[tuple[str, str, str]] = field(default_factory=list)


def migrate_legacy(conn: sqlite3.Connection) -> bool:
    """执行一次性旧数据升级。返回是否真正执行。调用方持有连接。"""
    row = conn.execute(
        "SELECT 1 FROM schema_migrations WHERE name=?", (MIGRATION_NAME,)
    ).fetchone()
    if row is not None:
        return False
    conn.execute("BEGIN IMMEDIATE")
    try:
        # 拿到写锁后复核：多进程同时启动时只有一个真正执行迁移。
        row = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE name=?", (MIGRATION_NAME,)
        ).fetchone()
        if row is not None:
            conn.execute("COMMIT")
            return False
        _do_migrate(conn)
        conn.execute(
            "INSERT INTO schema_migrations(name) VALUES (?)", (MIGRATION_NAME,)
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise


# ---------------------------------------------------------------- 版本身份映射
def _legacy_rows(conn: sqlite3.Connection, version_id: int) -> list[tuple[str, str, str]]:
    """旧版本里实际落库的 (图像号, 患者号, 类别)，保持插入顺序。"""
    return [
        (r["image_no"], r["patient_no"], r["class_name"])
        for r in conn.execute(
            "SELECT image_no, patient_no, class_name FROM images "
            "WHERE version_id=? ORDER BY id",
            (version_id,),
        ).fetchall()
    ]


def _build_mapping(conn: sqlite3.Connection, version: sqlite3.Row) -> VersionMapping:
    old_rows = _legacy_rows(conn, version["id"])
    mapping = VersionMapping(version_id=version["id"])
    try:
        parsed = parse_images_csv(version["image_csv"])
    except Exception:  # noqa: BLE001 - 原始 CSV 无法解析：按无法还原处理
        mapping.corrupt = True
        mapping.parse_error = True
        return mapping
    mapping.true_rows = [(r.image_no, r.patient_no, r.class_name) for r in parsed]
    # 旧派生表为空时无法比对，交由逐条明细的证据判定决定是否受影响；
    # 非空且与真实解析不一致即为受影响版本。
    mapping.corrupt = bool(old_rows) and old_rows != mapping.true_rows
    return mapping


def _infer_candidates(
    legacy: tuple[str, str, str], true_rows: list[tuple[str, str, str]]
) -> list[str]:
    """依据原始 CSV 的字段值，给出旧记录可能对应的全部真实图像号。

    旧解析器按固定列序取字段，因此旧三元组一定是某条真实三元组的排列
    （正常列序时相等，错位时前两列互换）。凡字段值集合与旧记录完全一致的
    真实行都是候选；只有候选收敛为同一图像号才算唯一对应。
    只做证据匹配，绝不在多个候选之间任选。
    """
    wanted = sorted(legacy)
    candidates: list[str] = []
    for img, pat, cls in true_rows:
        if sorted((img, pat, cls)) == wanted and img not in candidates:
            candidates.append(img)
    return candidates



def _resolve_entry(
    entry: sqlite3.Row, mapping: VersionMapping
) -> tuple[str | None, list[str], str]:
    """返回 (真实图像号, 候选图像号, 判定)。判定：resolved | ambiguous | unresolved。

    正常数据（旧解析器本来就读对了列）会命中“字段值完全相同的唯一真实行”；
    错位数据按原始 CSV 的字段值集合匹配；只有唯一命中才视为还原成功。
    """
    key = (entry["image_no"], entry["patient_no"], entry["class_name"])
    candidates = [] if mapping.parse_error else _infer_candidates(key, mapping.true_rows)
    if len(candidates) == 1:
        return candidates[0], candidates, "resolved"
    if len(candidates) > 1:
        return None, candidates, "ambiguous"
    return None, [], "unresolved"


# ---------------------------------------------------------------- 主升级逻辑
def _do_migrate(conn: sqlite3.Connection) -> None:
    # 旧登记键完整留档（不删除原值，只是不再作为权威身份）
    conn.execute(
        "INSERT OR IGNORE INTO legacy_registry_archive"
        "(image_no, side, first_manifest_id, locked_at) "
        "SELECT image_no, side, first_manifest_id, locked_at FROM side_registry"
    )

    # 本模块的派生结果全部重建；源数据（input_versions.image_csv、
    # manifest_entries、清单与作业编号）从不修改。
    conn.execute("DELETE FROM legacy_identity_records")
    conn.execute("DELETE FROM identity_conflicts")
    conn.execute("DELETE FROM manifest_quarantine")
    conn.execute("DELETE FROM side_registry")

    versions = {
        r["id"]: r
        for r in conn.execute("SELECT * FROM input_versions").fetchall()
    }
    mappings = {vid: _build_mapping(conn, v) for vid, v in versions.items()}

    # 每张真实图像 -> {side: [(manifest_id, entry_id, evidence_id)]}
    sides_by_image: dict[str, dict[str, list[tuple[int, int, int]]]] = {}
    # 无唯一对应的历史记录，用于“涉及即关联冲突”
    unresolved_records: list[dict] = []
    # 真实图像 -> 说明为何不能确定历史侧别
    evidence_conflicts: dict[str, list[str]] = {}
    manifest_reasons: dict[int, list[str]] = {}
    manifest_details: dict[int, dict] = {}

    def note_conflict(image_no: str, reason: str) -> None:
        reasons = evidence_conflicts.setdefault(image_no, [])
        if reason not in reasons:
            reasons.append(reason)

    def note_manifest(mid: int, reason: str, evidence_id: int | None = None,
                      images: list[str] | None = None) -> None:
        reasons = manifest_reasons.setdefault(mid, [])
        if reason not in reasons:
            reasons.append(reason)
        detail = manifest_details.setdefault(mid, {"evidence_ids": [], "images": []})
        if evidence_id is not None:
            detail["evidence_ids"].append(evidence_id)
        for img in images or []:
            if img and img not in detail["images"]:
                detail["images"].append(img)

    manifests = conn.execute(
        "SELECT * FROM manifests WHERE status='published' ORDER BY id"
    ).fetchall()
    for manifest in manifests:
        mid = manifest["id"]
        mapping = mappings.get(manifest["version_id"])
        if mapping is not None and mapping.corrupt:
            note_manifest(
                mid,
                "原始上传 CSV 的表头列序与旧解析器不一致：该清单按旧登记键生成，"
                "图像号/患者号字段可能错位；已依据原始 CSV 与原发布明细还原真实身份，"
                "本旧清单仅保留原始明细与编号，下载停供。",
            )
        entries = conn.execute(
            "SELECT * FROM manifest_entries WHERE manifest_id=? ORDER BY id", (mid,)
        ).fetchall()
        for entry in entries:
            if mapping is None:
                image_no, candidates, resolution = None, [], "unresolved"
            else:
                image_no, candidates, resolution = _resolve_entry(entry, mapping)
            cur = conn.execute(
                "INSERT INTO legacy_identity_records("
                "manifest_id, version_id, entry_id, legacy_image_no, legacy_patient_no, "
                "legacy_class_name, resolved_image_no, candidates, resolution, side, note) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    mid,
                    manifest["version_id"],
                    entry["id"],
                    entry["image_no"],
                    entry["patient_no"],
                    entry["class_name"],
                    image_no,
                    json.dumps(candidates, ensure_ascii=False),
                    resolution,
                    entry["side"],
                    None,
                ),
            )
            evidence_id = cur.lastrowid
            if resolution == "resolved" and image_no is not None:
                sides_by_image.setdefault(image_no, {}).setdefault(
                    entry["side"], []
                ).append((mid, entry["id"], evidence_id))
                note_manifest(mid, "—", evidence_id, [image_no])
            else:
                unresolved_records.append(
                    {
                        "evidence_id": evidence_id,
                        "manifest_id": mid,
                        "version_id": manifest["version_id"],
                        "legacy_image_no": entry["image_no"],
                        "legacy_patient_no": entry["patient_no"],
                        "legacy_class_name": entry["class_name"],
                        "candidates": candidates,
                        "resolution": resolution,
                        "side": entry["side"],
                    }
                )
                cand_text = "、".join(candidates) if candidates else "（无候选）"
                reason = (
                    f"原发布明细中的历史登记键 {entry['image_no']} 无法唯一对应到真实图像"
                    f"（候选：{cand_text}），该证据被保留；涉及候选图像的划分将判关联冲突。"
                )
                note_manifest(mid, reason, evidence_id, candidates)
                # 无法唯一对应 → 所有候选都不得被任选一侧
                for cand in candidates:
                    note_conflict(cand, reason)
                if not candidates:
                    # 解析失败时连候选都无法确定：旧字段值本身作为保守候选，
                    # 避免旧键被当成无关图像重新划分。
                    for value in {entry["image_no"], entry["patient_no"]}:
                        note_conflict(value, reason)
                    note_manifest(mid, reason, evidence_id, [])

    # 一图曾发布到两侧 → 关联冲突，绝不进入永久登记
    for image_no, side_evidence in sides_by_image.items():
        sides = sorted(side_evidence.keys())
        if len(sides) <= 1:
            continue
        evidence = [
            {
                "side": side,
                "manifest_id": mid,
                "entry_id": entry_id,
                "evidence_id": evidence_id,
            }
            for side, items in side_evidence.items()
            for mid, entry_id, evidence_id in items
        ]
        reason = (
            f"图像 {image_no} 在历史发布证据中曾出现在"
            + "、".join("验证侧" if s == "val" else "训练侧" for s in sides)
            + "（"
            + "、".join(f"清单 #{e['manifest_id']}" for e in evidence)
            + "），无法唯一确定历史侧别。"
        )
        note_conflict(image_no, reason)
        for e in evidence:
            note_manifest(e["manifest_id"], reason, e["evidence_id"], [image_no])

    for image_no, reasons in sorted(evidence_conflicts.items()):
        side_evidence = sides_by_image.get(image_no, {})
        sides = sorted(side_evidence.keys())
        reason = " ".join(reasons) + " 涉及该图像的划分将判关联冲突，不任选一侧。"
        evidence = [
            {
                "side": side,
                "manifest_id": mid,
                "entry_id": entry_id,
                "evidence_id": evidence_id,
            }
            for side, items in side_evidence.items()
            for mid, entry_id, evidence_id in items
        ] + [
            {
                "side": rec["side"],
                "manifest_id": rec["manifest_id"],
                "evidence_id": rec["evidence_id"],
                "candidates": rec["candidates"],
            }
            for rec in unresolved_records
            if image_no in rec["candidates"]
            or (not rec["candidates"]
                and image_no in (rec["legacy_image_no"], rec["legacy_patient_no"]))
        ]
        conn.execute(
            "INSERT OR REPLACE INTO identity_conflicts(image_no, sides, reason, evidence) "
            "VALUES (?,?,?,?)",
            (
                image_no,
                json.dumps(sides),
                reason,
                json.dumps(evidence, ensure_ascii=False),
            ),
        )

    # 仅单一历史侧别且无任何证据歧义的真实图像重建为永久登记；first_manifest 取最早清单
    for image_no, side_evidence in sorted(sides_by_image.items()):
        if image_no in evidence_conflicts or len(side_evidence) != 1:
            continue
        side, items = next(iter(side_evidence.items()))
        first_manifest = min(mid for mid, _, _ in items)
        created = conn.execute(
            "SELECT created_at FROM manifests WHERE id=?", (first_manifest,)
        ).fetchone()
        conn.execute(
            "INSERT INTO side_registry(image_no, side, first_manifest_id, locked_at) "
            "VALUES (?,?,?,?)",
            (
                image_no,
                side,
                first_manifest,
                created["created_at"] if created else None,
            ),
        )

    # 受影响清单：保留原编号与明细，仅停供下载，并给出可查原因
    for mid, reasons in manifest_reasons.items():
        clean = [r for r in reasons if r != "—"]
        if not clean:
            continue
        detail = manifest_details.get(mid, {})
        conn.execute(
            "INSERT INTO manifest_quarantine(manifest_id, reason, detail) "
            "VALUES (?,?,?)",
            (
                mid,
                "；".join(clean),
                json.dumps(
                    {
                        "evidence_ids": sorted(set(detail.get("evidence_ids", []))),
                        "images": sorted(set(detail.get("images", []))),
                    },
                    ensure_ascii=False,
                ),
            ),
        )
        # 原清单编号与明细不变，只标记停供（published 状态本身保留）
        conn.execute(
            "UPDATE manifests SET quarantined=1, reason=? WHERE id=?",
            ("；".join(clean), mid),
        )
        # 对应作业作废：保留状态/原因可查，但不再占“活动作业”唯一位，
        # 允许按真实身份重新划分同一版本。
        conn.execute(
            "UPDATE partition_jobs SET voided=1 WHERE manifest_id=?", (mid,)
        )

    # 修复派生 images / dup_edges（原始 CSV 全文保持不变）。
    # 重复关系的表头列序错误与图像列序错误相互独立，因此所有版本都按修复后的
    # 解析器重建 dup_edges（正常数据的解析结果与旧值一致，幂等）。
    for vid, mapping in mappings.items():
        if mapping.corrupt and not mapping.parse_error:
            conn.execute("DELETE FROM images WHERE version_id=?", (vid,))
            conn.executemany(
                "INSERT INTO images(version_id, image_no, patient_no, class_name) "
                "VALUES (?,?,?,?)",
                [(vid, img, pat, cls) for img, pat, cls in mapping.true_rows],
            )
        version = versions[vid]
        try:
            edges = parse_dups_csv(version["dup_csv"])
        except InputError:
            continue
        conn.execute("DELETE FROM dup_edges WHERE version_id=?", (vid,))
        conn.executemany(
            "INSERT INTO dup_edges(version_id, image_a, image_b) VALUES (?,?,?)",
            [(vid, a, b) for a, b in edges],
        )


__all__ = ["migrate_legacy", "MIGRATION_NAME", "VersionMapping"]

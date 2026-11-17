"""纯函数级：历史身份证据匹配与隔离不变量。"""
import json

from app.db import get_db
from app.migration import _infer_candidates
from app.parser import ImageRow, validate_and_build
from app.solver import SolveError, solve
from app.service import Service
from tests.conftest import make_csv, make_dup


def test_infer_candidates_distinguishes_unique_and_ambiguous():
    true_rows = [
        ("IMG001", "P001", "肿瘤"),
        ("IMG002", "P002", "肿瘤"),
        ("P001", "IMG001", "肿瘤"),   # 与第一行互为排列
    ]
    # 正常列序的旧记录：唯一命中
    assert _infer_candidates(("IMG002", "P002", "肿瘤"), true_rows) == ["IMG002"]
    # 错位旧记录：唯一命中 IMG002 的排列不存在，但对 IMG001 存在两个候选
    assert sorted(_infer_candidates(("P001", "IMG001", "肿瘤"), true_rows)) == [
        "IMG001", "P001"]
    # 完全不存在
    assert _infer_candidates(("ZZZ", "QQQ", "肿瘤"), true_rows) == []


def test_solver_conflict_image_blocks_component_even_if_unlocked():
    rows = [ImageRow("I1", "P1", "A"), ImageRow("I2", "P2", "A")]
    comps, classes = validate_and_build(rows, [])
    try:
        solve(comps, classes, {"A": 1}, {}, {"I1": "证据冲突测试"})
    except SolveError as exc:
        assert exc.stage == "conflict"
        assert exc.detail["conflict_images"] == ["I1"]
    else:
        raise AssertionError("应判关联冲突")


def test_quarantined_manifest_not_counted_for_download_but_entries_intact(tmp_path):
    db = str(tmp_path / "q.db")
    svc = Service(db, synchronous=True)
    svc.create_version(make_csv([("I1", "P1", "A")]), make_dup([]), {"A": 1})
    svc.start_partition()
    m = svc.current_manifest()
    conn = get_db(db)
    try:
        conn.execute("INSERT INTO manifest_quarantine(manifest_id,reason,detail) "
                     "VALUES (?,?,?)", (m["id"], "测试停供", "{}"))
        conn.execute("UPDATE manifests SET quarantined=1 WHERE id=?", (m["id"],))
        conn.commit()
    finally:
        conn.close()
    assert svc.current_manifest() is None
    assert svc.current_manifest_quarantine() is not None
    # 原始明细仍在
    assert len(svc.get_manifest_entries(m["id"])) == 1
    svc.close()

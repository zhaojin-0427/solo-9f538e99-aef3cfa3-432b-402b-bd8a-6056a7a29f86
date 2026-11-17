"""旧数据升级：表头列序错位的历史登记还原、冲突保留、隔离与幂等。

这些用例先构造“旧解析器写出的数据库”（按固定列序取字段），再用新代码启动，
验证启动迁移能依据原始 CSV 与原发布明细还原真实身份。
"""
import json

from app.db import get_db, init_db
from app.service import Service
from tests.conftest import make_csv, make_dup

REORDERED_HEADERS = ("患者号", "图像号", "类别")


def legacy_csv(rows):
    """表头为 患者号,图像号,类别，数据列序随表头。"""
    lines = [",".join(REORDERED_HEADERS)]
    for img, pat, cls in rows:
        lines.append(f"{pat},{img},{cls}")
    return "\n".join(lines) + "\n"


def legacy_db(path, versions, manifests):
    """按旧解析行为写库：image_no=第一列(患者号)，patient_no=第二列(图像号)。

    versions: [(image_text, dup_text, quotas)]，最后一个为 active。
    manifests: [(version_index, [(old_image_no, old_patient_no, class, cid, side)])]
    """
    init_db(path)
    conn = get_db(path)
    try:
        for t in (
            "manifest_entries", "manifest_quarantine", "manifests", "partition_jobs",
            "schema_migrations", "side_registry", "identity_conflicts",
            "legacy_identity_records", "legacy_registry_archive", "dup_edges",
            "images", "input_versions",
        ):
            conn.execute(f"DELETE FROM {t}")
        vids = []
        for i, (image_text, dup_text, quotas) in enumerate(versions):
            status = "active" if i == len(versions) - 1 else "superseded"
            cur = conn.execute(
                "INSERT INTO input_versions(image_csv, dup_csv, image_count, class_names, "
                "quotas, status) VALUES (?,?,?,?,?,?)",
                (
                    image_text,
                    dup_text,
                    len([l for l in image_text.strip().split("\n")[1:] if l.strip()]),
                    ",".join(sorted(quotas)),
                    json.dumps(quotas, ensure_ascii=False),
                    status,
                ),
            )
            vids.append(cur.lastrowid)
        for vi, entries in manifests:
            vid = vids[vi]
            mid = conn.execute(
                "INSERT INTO manifests(version_id, status) VALUES (?,'published')", (vid,)
            ).lastrowid
            for image_no, patient_no, cls, cid, side in entries:
                conn.execute(
                    "INSERT INTO manifest_entries(manifest_id, image_no, patient_no, "
                    "class_name, component_id, side) VALUES (?,?,?,?,?,?)",
                    (mid, image_no, patient_no, cls, cid, side),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO side_registry(image_no, side, first_manifest_id) "
                    "VALUES (?,?,?)",
                    (image_no, side, mid),
                )
        # 旧版 images 表：image_no=第一列、patient_no=第二列
        for i, (image_text, _, _) in enumerate(versions):
            data = [l.split(",") for l in image_text.strip().split("\n")[1:] if l.strip()]
            for cols in data:
                conn.execute(
                    "INSERT INTO images(version_id, image_no, patient_no, class_name) "
                    "VALUES (?,?,?,?)",
                    (vids[i], cols[0], cols[1], cols[2]),
                )
        conn.commit()
    finally:
        conn.close()


def read_db(path):
    conn = get_db(path)
    try:
        return {
            "registry": {
                r["image_no"]: r["side"]
                for r in conn.execute("SELECT * FROM side_registry")
            },
            "conflicts": {
                r["image_no"]: json.loads(r["sides"])
                for r in conn.execute("SELECT * FROM identity_conflicts")
            },
            "quarantine": {
                r["manifest_id"]: r["reason"]
                for r in conn.execute("SELECT * FROM manifest_quarantine")
            },
            "legacy": {
                r["entry_id"]: (r["resolved_image_no"], r["resolution"])
                for r in conn.execute("SELECT * FROM legacy_identity_records")
            },
            "manifests": {
                r["id"]: r["quarantined"]
                for r in conn.execute("SELECT * FROM manifests")
            },
            "images": {
                (r["version_id"], r["image_no"]): r["patient_no"]
                for r in conn.execute("SELECT * FROM images")
            },
        }
    finally:
        conn.close()


def rows6():
    return [
        ("IMG001", "P001", "肿瘤"), ("IMG002", "P002", "肿瘤"),
        ("IMG003", "P003", "肿瘤"), ("IMG004", "P004", "正常"),
        ("IMG005", "P005", "正常"), ("IMG006", "P006", "正常"),
    ]


def test_header_order_respected_end_to_end(tmp_path):
    """按表头识别列序；正常列序重传同一批图像不改变真实图像侧别。"""
    db = str(tmp_path / "new.db")
    svc = Service(db, synchronous=True)
    rows = rows6()
    text = legacy_csv(rows)
    svc.create_version(text, make_dup([]), {"肿瘤": 2, "正常": 1})
    svc.start_partition()
    first = {e["image_no"]: e["side"] for e in svc.get_manifest_entries(
        svc.current_manifest()["id"])}
    assert set(first) == {r[0] for r in rows}
    assert all(img.startswith("IMG") for img in first)
    # 按“图像号,患者号,类别”的正常列序重传同一批图像：真实图像仍锁在同一侧
    svc.create_version(make_csv(rows), make_dup([]), {"肿瘤": 2, "正常": 1})
    svc.start_partition()
    second = {e["image_no"]: e["side"] for e in svc.get_manifest_entries(
        svc.current_manifest()["id"])}
    assert second == first
    svc.close()


def test_legacy_db_identity_restored_from_original_csv(tmp_path):
    """旧库（身份错位）启动后按原始 CSV + 原发布明细还原真实登记。"""
    db = str(tmp_path / "old.db")
    rows = rows6()
    text = legacy_csv(rows)
    legacy_entries = [
        ("P001", "IMG001", "肿瘤", 0, "val"),
        ("P002", "IMG002", "肿瘤", 1, "val"),
        ("P003", "IMG003", "肿瘤", 2, "train"),
        ("P004", "IMG004", "正常", 3, "train"),
        ("P005", "IMG005", "正常", 4, "val"),
        ("P006", "IMG006", "正常", 5, "train"),
    ]
    legacy_db(db, [(text, "", {"肿瘤": 2, "正常": 1})], [(0, legacy_entries)])
    # 旧库里的患者号被当成图像号、图像号被当成患者号
    before = read_db(db)
    assert before["registry"] == {e[0]: e[4] for e in legacy_entries}
    assert before["images"][(1, "P001")] == "IMG001"

    svc = Service(db, synchronous=True)
    data = read_db(db)
    expected = {f"IMG00{i}": side for i, side in [
        (1, "val"), (2, "val"), (3, "train"), (4, "train"), (5, "val"), (6, "train")]}
    assert data["registry"] == expected
    # 旧登记键留档保留
    conn = get_db(db)
    try:
        archived = {r["image_no"] for r in conn.execute(
            "SELECT image_no FROM legacy_registry_archive")}
    finally:
        conn.close()
    assert archived == {e[0] for e in legacy_entries}
    # 派生 images 表按真实解析修复，原始 CSV 全文不变
    assert data["images"][(1, "IMG001")] == "P001"
    assert "P001" not in {k[1] for k in data["images"]}
    # 旧清单受影响 → 停供，但原编号与明细保留
    assert data["quarantine"]
    assert data["manifests"][1] == 1
    conn = get_db(db)
    try:
        assert conn.execute(
            "SELECT COUNT(*) c FROM manifest_entries WHERE manifest_id=1"
        ).fetchone()["c"] == 6
        raw = conn.execute("SELECT image_csv FROM input_versions WHERE id=1").fetchone()[0]
    finally:
        conn.close()
    assert raw == text

    # 按正常列序重传同批图像：真实图像侧别全部保持
    svc.create_version(make_csv(rows), make_dup([]), {"肿瘤": 2, "正常": 1})
    svc.start_partition()
    assert svc.list_jobs()[0]["status"] == "succeeded"
    new = {e["image_no"]: e["side"] for e in svc.get_manifest_entries(
        svc.current_manifest()["id"])}
    assert new == expected
    svc.close()


def test_ambiguous_legacy_entry_preserves_evidence_and_blocks_partition(tmp_path):
    """旧明细无法唯一对应时保留候选证据，并在涉及它的划分中报关联冲突。"""
    db = str(tmp_path / "amb.db")
    # 真实数据两行的字段值多元组完全相同，仅靠旧字段值无法区分
    rows = [("IMG001", "P001", "肿瘤"), ("P001", "IMG001", "肿瘤"),
            ("IMG009", "P009", "正常")]
    text = make_csv(rows)
    legacy_db(
        db,
        [(text, "", {"肿瘤": 1, "正常": 0})],
        [(0, [
            ("P001", "IMG001", "肿瘤", 0, "val"),
            ("P009", "IMG009", "正常", 1, "train"),
        ])],
    )
    # 旧库缺了一行 images（模拟历史数据不完整），使按位置对应失效
    conn = get_db(db)
    try:
        conn.execute("DELETE FROM images WHERE image_no='P001' AND patient_no='IMG001'")
        conn.commit()
    finally:
        conn.close()

    svc = Service(db, synchronous=True)
    data = read_db(db)
    resolved, resolution = data["legacy"][1]
    assert resolved is None
    assert resolution == "ambiguous"
    assert data["conflicts"], "歧义证据必须形成关联冲突记录"
    # 涉及候选图像的划分直接判关联冲突，而不是任选一侧
    svc.create_version(text, make_dup([]), {"肿瘤": 1, "正常": 0})
    svc.start_partition()
    job = svc.list_jobs()[0]
    assert job["status"] == "failed"
    assert job["fail_stage"] == "conflict"
    assert "关联冲突" in job["reason"]
    # 旧清单停供，证据仍在
    assert data["quarantine"]
    svc.close()


def test_one_image_published_to_both_sides_never_registered(tmp_path):
    """一图历史证据曾出现在两侧：不写永久登记，停供旧清单并在划分中报冲突。"""
    db = str(tmp_path / "both.db")
    rows = [
        ("IMG001", "P001", "肿瘤"), ("IMG002", "P002", "肿瘤"),
        ("IMG003", "P003", "正常"),
    ]
    text = make_csv(rows)
    # 两份不同版本的历史清单：IMG001 曾在验证侧，也曾在训练侧
    legacy_db(
        db,
        [(text, "", {"肿瘤": 1, "正常": 0}), (text, "", {"肿瘤": 1, "正常": 0})],
        [
            (0, [("P001", "IMG001", "肿瘤", 0, "val"),
                 ("P002", "IMG002", "肿瘤", 1, "train"),
                 ("P003", "IMG003", "正常", 2, "train")]),
            (1, [("P001", "IMG001", "肿瘤", 0, "train"),
                 ("P002", "IMG002", "肿瘤", 1, "train"),
                 ("P003", "IMG003", "正常", 2, "train")]),
        ],
    )
    Service(db, synchronous=True).close()  # 触发迁移
    data = read_db(db)
    assert "IMG001" not in data["registry"]      # 不在任一侧登记
    assert data["conflicts"]["IMG001"] == ["train", "val"]
    assert data["quarantine"]                    # 两份清单均停供
    conn = get_db(db)
    try:
        # 证据指向具体清单与明细
        evidence = json.loads(conn.execute(
            "SELECT evidence FROM identity_conflicts WHERE image_no='IMG001'"
        ).fetchone()["evidence"])
    finally:
        conn.close()
    assert {e["manifest_id"] for e in evidence} == {1, 2}
    assert all(e.get("evidence_id") for e in evidence)


def test_unaffected_manifest_still_served(tmp_path):
    """未受影响的当前清单照常供数；仅受影响的旧清单停供。"""
    db = str(tmp_path / "mix.db")
    good_rows = [("IMG101", "P101", "肿瘤"), ("IMG102", "P102", "正常")]
    good_text = make_csv(good_rows)
    bad_rows = [("IMG001", "P001", "肿瘤"), ("IMG002", "P002", "正常")]
    bad_text = legacy_csv(bad_rows)
    legacy_db(
        db,
        [(bad_text, "", {"肿瘤": 1, "正常": 0}), (good_text, "", {"肿瘤": 1, "正常": 0})],
        [
            (0, [("P001", "IMG001", "肿瘤", 0, "train"),
                 ("P002", "IMG002", "正常", 1, "train")]),
            (1, [("IMG101", "P101", "肿瘤", 0, "train"),
                 ("IMG102", "P102", "正常", 1, "val")]),
        ],
    )
    svc = Service(db, synchronous=True)
    data = read_db(db)
    assert data["manifests"][1] == 1          # 旧清单隔离
    assert data["manifests"][2] == 0
    cur = svc.current_manifest()
    assert cur is not None
    assert cur["version_id"] == 2
    # 未受影响的当前清单仍可下载
    assert cur["status"] == "published"
    svc.close()


def test_migration_is_idempotent_across_restarts(tmp_path):
    """反复重启不会重复改写登记，也不产生重复证据。"""
    db = str(tmp_path / "idem.db")
    rows = rows6()
    text = legacy_csv(rows)
    legacy_db(
        db,
        [(text, "", {"肿瘤": 2, "正常": 1})],
        [(0, [(f"P00{i}", f"IMG00{i}",
               "肿瘤" if i <= 3 else "正常", i, "val" if i <= 2 else "train")
              for i in range(1, 7)])],
    )
    snapshots = []
    for _ in range(3):
        Service(db, synchronous=True).close()
        snapshots.append(read_db(db))
    assert snapshots[0] == snapshots[1] == snapshots[2]
    assert snapshots[0]["registry"]


def test_quarantined_current_version_can_republish_clean_manifest(tmp_path):
    """受影响当前版本仍可按真实身份重新划分，旧清单保持停供与证据。"""
    db = str(tmp_path / "repub.db")
    rows = rows6()
    text = legacy_csv(rows)
    legacy_db(
        db,
        [(text, "", {"肿瘤": 2, "正常": 1})],
        [(0, [(f"P00{i}", f"IMG00{i}", "肿瘤" if i <= 3 else "正常", i,
               "val" if i in (1, 2, 5) else "train") for i in range(1, 7)])],
    )
    svc = Service(db, synchronous=True)
    assert svc.current_manifest() is None          # 旧清单停供
    assert svc.current_manifest_quarantine() is not None
    # 同一版本直接重新划分：按真实身份与永久登记精确满足配额
    svc.start_partition()
    job = svc.list_jobs()[0]
    assert job["status"] == "succeeded", job["reason"]
    clean = svc.current_manifest()
    assert clean is not None and clean["quarantined"] == 0
    sides = {e["image_no"]: e["side"] for e in svc.get_manifest_entries(clean["id"])}
    registry = {r["image_no"]: r["side"] for r in svc.registry_snapshot()}
    for img, side in sides.items():
        assert registry[img] == side                 # 新划分服从永久侧别
    val = [img for img, s in sides.items() if s == "val"]
    assert len(val) == 3
    # 旧清单仍停供、编号与证据不变
    data = read_db(db)
    assert data["manifests"][1] == 1
    assert data["quarantine"]
    svc.close()


def test_quarantined_manifest_download_blocked_and_reason_visible(tmp_path):
    """浏览器可查停供原因；受影响旧清单下载被拒；未受影响的当前清单可下载。"""
    from app.web import create_app

    db = str(tmp_path / "web.db")
    bad_rows = [("IMG001", "P001", "肿瘤"), ("IMG002", "P002", "正常")]
    good_rows = [("IMG101", "P101", "肿瘤"), ("IMG102", "P102", "正常")]
    legacy_db(
        db,
        [(legacy_csv(bad_rows), "", {"肿瘤": 1, "正常": 0}),
         (make_csv(good_rows), "", {"肿瘤": 1, "正常": 0})],
        [
            (0, [("P001", "IMG001", "肿瘤", 0, "train"),
                 ("P002", "IMG002", "正常", 1, "train")]),
            (1, [("IMG101", "P101", "肿瘤", 0, "train"),
                 ("IMG102", "P102", "正常", 1, "val")]),
        ],
    )
    app = create_app(db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        hist = c.get("/history").data.decode("utf-8")
        assert "历史-身份受影响-停供" in hist
        assert "旧发布明细" in hist
        # 旧清单下载停供
        resp = c.get("/manifests/1/download/val", follow_redirects=False)
        assert resp.status_code == 302
        # 当前未受影响的清单可下载
        resp2 = c.get("/manifests/2/download/val")
        assert resp2.status_code == 200
        assert "IMG102" in resp2.data.decode("utf-8")
    app.extensions["service"].close()


def test_legacy_dup_header_order_and_deleted_images_recovered(tmp_path):
    """旧重复关系表头列序错位也按原文件修复；已删除图像的历史不遗漏。"""
    db = str(tmp_path / "dup.db")
    rows = [
        ("IMG001", "P001", "肿瘤"), ("IMG002", "P002", "肿瘤"),
        ("IMG003", "P003", "正常"),
    ]
    text = make_csv(rows)
    # 旧 dup CSV：表头 图像号B,图像号A，旧解析按列序写入 a=IMG002,b=IMG001
    old_dup = "图像号B,图像号A\nIMG002,IMG001\n"
    legacy_db(
        db,
        [(text, old_dup, {"肿瘤": 1, "正常": 0})],
        [(0, [
            ("P001", "IMG001", "肿瘤", 0, "val"),
            ("P002", "IMG002", "肿瘤", 0, "val"),
            ("P003", "IMG003", "正常", 1, "train"),
        ])],
    )
    # 模拟“已删除图像”：当前版本 images 表里缺一张，但历史明细有它
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO dup_edges(version_id,image_a,image_b) VALUES (1,'IMG002','IMG001')"
        )
        conn.execute("DELETE FROM images WHERE image_no='P002'")
        conn.commit()
    finally:
        conn.close()

    Service(db, synchronous=True).close()
    data = read_db(db)
    # 已删除图像的历史仍被还原（不因当前版本缺失而遗漏）
    assert data["registry"]["IMG002"] == "val"
    assert data["registry"]["IMG001"] == "val"
    conn = get_db(db)
    try:
        edges = conn.execute(
            "SELECT image_a, image_b FROM dup_edges WHERE version_id=1"
        ).fetchall()
        rows_now = {r["image_no"] for r in conn.execute(
            "SELECT image_no FROM images WHERE version_id=1")}
    finally:
        conn.close()
    assert [(e["image_a"], e["image_b"]) for e in edges] == [("IMG001", "IMG002")]
    assert rows_now == {"IMG001", "IMG002", "IMG003"}


def test_migration_failure_leaves_no_partial_repair(tmp_path, monkeypatch):
    """升级中途失败：整体回滚，旧数据与旧登记原样，下次启动可重试。"""
    import app.migration as migration

    db = str(tmp_path / "rollback.db")
    rows = rows6()
    text = legacy_csv(rows)
    legacy_db(
        db,
        [(text, "", {"肿瘤": 2, "正常": 1})],
        [(0, [(f"P00{i}", f"IMG00{i}", "肿瘤" if i <= 3 else "正常", i, "val")
              for i in range(1, 7)])],
    )
    before = read_db(db)
    before_images = {k: v for k, v in before["images"].items()}

    real = migration._do_migrate

    def boom(conn):
        real(conn)
        raise RuntimeError("模拟升级中途断电")

    monkeypatch.setattr(migration, "_do_migrate", boom)
    try:
        Service(db, synchronous=True)
    except RuntimeError:
        pass
    else:
        raise AssertionError("升级失败应抛出，不得当作成功")
    monkeypatch.setattr(migration, "_do_migrate", real)

    after = read_db(db)
    # 全部派生结果回滚：旧登记、旧 images、无冲突、无停供、无迁移标记
    assert after["registry"] == before["registry"]
    assert after["images"] == before_images
    assert after["conflicts"] == {}
    assert after["quarantine"] == {}
    assert after["legacy"] == {}
    conn = get_db(db)
    try:
        assert conn.execute("SELECT COUNT(*) c FROM schema_migrations").fetchone()["c"] == 0
    finally:
        conn.close()

    # 修复后重启可完成升级
    Service(db, synchronous=True).close()
    fixed = read_db(db)
    assert fixed["registry"]["IMG001"] == "val"
    assert fixed["legacy"]


def test_mixed_old_and_new_manifests_judged_consistently(tmp_path):
    """新旧记录混存：新代码产生的正常清单不被误伤，旧错位清单单独停供。"""
    db = str(tmp_path / "mixed.db")
    # 先用新代码正常发布一份清单（正常列序，身份正确）
    svc = Service(db, synchronous=True)
    good_rows = [("IMG201", "P201", "肿瘤"), ("IMG202", "P202", "正常")]
    svc.create_version(make_csv(good_rows), make_dup([]), {"肿瘤": 1, "正常": 0})
    svc.start_partition()
    good_manifest = svc.current_manifest()
    good_sides = {e["image_no"]: e["side"] for e in svc.get_manifest_entries(
        good_manifest["id"])}
    svc.close()

    # 追加一份“旧代码”产生的错位清单（另一个历史版本）
    conn = get_db(db)
    try:
        bad_rows = [("IMG301", "P301", "肿瘤"), ("IMG302", "P302", "正常")]
        text = legacy_csv(bad_rows)
        vid = conn.execute(
            "INSERT INTO input_versions(image_csv,dup_csv,image_count,class_names,"
            "quotas,status) VALUES (?,?,?,?,?,'superseded')",
            (text, "", 2, "肿瘤,正常", json.dumps({"肿瘤": 0, "正常": 1})),
        ).lastrowid
        mid = conn.execute(
            "INSERT INTO manifests(version_id,status) VALUES (?,'published')", (vid,)
        ).lastrowid
        for i, (old_img, old_pat, cls) in enumerate(
                [("P301", "IMG301", "肿瘤"), ("P302", "IMG302", "正常")]):
            conn.execute(
                "INSERT INTO manifest_entries(manifest_id,image_no,patient_no,"
                "class_name,component_id,side) VALUES (?,?,?,?,?,?)",
                (mid, old_img, old_pat, cls, i, "train"),
            )
            conn.execute(
                "INSERT OR IGNORE INTO side_registry(image_no,side,first_manifest_id) "
                "VALUES (?,?,?)", (old_img, "train", mid),
            )
        # 旧 images 行（错位）
        for old_img, old_pat, cls in [("P301", "IMG301", "肿瘤"), ("P302", "IMG302", "正常")]:
            conn.execute(
                "INSERT INTO images(version_id,image_no,patient_no,class_name) "
                "VALUES (?,?,?,?)", (vid, old_img, old_pat, cls))
        # 清掉迁移标记，模拟“新库中混入旧记录”后第一次重启升级
        conn.execute("DELETE FROM schema_migrations")
        conn.commit()
    finally:
        conn.close()

    svc2 = Service(db, synchronous=True)
    data = read_db(db)
    # 新记录保持原样且未停供
    assert data["manifests"][good_manifest["id"]] == 0
    assert {img: data["registry"][img] for img in good_sides} == good_sides
    # 旧记录被还原并停供
    assert data["manifests"][2] == 1
    assert data["registry"]["IMG301"] == "train"
    assert data["registry"]["IMG302"] == "train"
    # 当前版本（新代码的版本）仍可正常供数
    assert svc2.current_manifest() is not None
    svc2.close()


def test_correction_during_migration_and_restart_judgment_holds(tmp_path):
    """迁移后仍有更正/发布交错与反复重启时，永久侧别与精确配额判定成立。"""
    db = str(tmp_path / "interleave.db")
    rows = rows6()
    text = legacy_csv(rows)
    legacy_db(
        db,
        [(text, "", {"肿瘤": 2, "正常": 1})],
        [(0, [(f"P00{i}", f"IMG00{i}", "肿瘤" if i <= 3 else "正常", i,
               "val" if i in (1, 2, 5) else "train") for i in range(1, 7)])],
    )
    svc = Service(db, synchronous=True)
    locked = {r["image_no"]: r["side"] for r in svc.registry_snapshot()}
    # 迁移后重启一次：登记与锁不变
    svc.close()
    svc = Service(db, synchronous=True)
    assert {r["image_no"]: r["side"] for r in svc.registry_snapshot()} == locked

    # 更正（新版本、同批图像、不同配额）：锁定导致不可达 → 失败且不发布
    svc.create_version(make_csv(rows), make_dup([]), {"肿瘤": 1, "正常": 0})
    svc.start_partition()
    job = svc.list_jobs()[0]
    assert job["status"] == "failed"
    assert job["fail_stage"] == "quota"
    assert svc.current_manifest() is None
    # 登记保持不变（失败不写入任何分配）
    assert {r["image_no"]: r["side"] for r in svc.registry_snapshot()} == locked

    # 再更正为与锁定一致的配额：精确成功，且逐图服从永久侧别
    val_counts = {}
    for img, side in locked.items():
        if side == "val":
            cls = dict((r[0], r[2]) for r in rows)[img]
            val_counts[cls] = val_counts.get(cls, 0) + 1
    svc.create_version(make_csv(rows), make_dup([]),
                       {"肿瘤": val_counts.get("肿瘤", 0),
                        "正常": val_counts.get("正常", 0)})
    svc.start_partition()
    assert svc.list_jobs()[0]["status"] == "succeeded"
    new = {e["image_no"]: e["side"] for e in svc.get_manifest_entries(
        svc.current_manifest()["id"])}
    assert new == locked
    svc.close()
    svc = Service(db, synchronous=True)      # 再次重启判定仍成立
    assert {r["image_no"]: r["side"] for r in svc.registry_snapshot()} == locked
    assert svc.current_manifest() is not None
    svc.close()


def test_registry_page_lists_conflicts_not_arbitrary_side(tmp_path):
    db = str(tmp_path / "reg.db")
    rows = [("IMG001", "P001", "肿瘤"), ("IMG002", "P002", "肿瘤")]
    text = make_csv(rows)
    legacy_db(
        db,
        [(text, "", {"肿瘤": 1, "正常": 0}), (text, "", {"肿瘤": 1, "正常": 0})],
        [(0, [("P001", "IMG001", "肿瘤", 0, "val")]),
         (1, [("P001", "IMG001", "肿瘤", 0, "train")])],
    )
    from app.web import create_app
    app = create_app(db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        page = c.get("/registry").data.decode("utf-8")
        assert "历史证据冲突" in page
        assert "IMG001" in page
        # 冲突图像不出现在登记表中（登记表无对应行）
        reg = {r["image_no"]: r["side"] for r in app.extensions["service"].registry_snapshot()}
        assert "IMG001" not in reg
    app.extensions["service"].close()


def test_old_schema_database_upgraded_in_place(tmp_path):
    """真实旧库（无 quarantined/voided 列、旧唯一索引）可直接升级，不留部分修复。"""
    db = str(tmp_path / "oldschema.db")
    # 用最小旧 schema 建库（模拟旧版本遗留文件）
    import sqlite3

    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE input_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            status TEXT NOT NULL DEFAULT 'active',
            image_csv TEXT NOT NULL, dup_csv TEXT NOT NULL,
            image_count INTEGER NOT NULL, class_names TEXT NOT NULL,
            quotas TEXT NOT NULL, note TEXT);
        CREATE TABLE images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id INTEGER NOT NULL REFERENCES input_versions(id),
            image_no TEXT NOT NULL, patient_no TEXT NOT NULL,
            class_name TEXT NOT NULL, UNIQUE(version_id, image_no));
        CREATE TABLE dup_edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id INTEGER NOT NULL REFERENCES input_versions(id),
            image_a TEXT NOT NULL, image_b TEXT NOT NULL);
        CREATE TABLE side_registry (
            image_no TEXT PRIMARY KEY,
            side TEXT NOT NULL CHECK (side IN ('train','val')),
            first_manifest_id INTEGER,
            locked_at TEXT NOT NULL DEFAULT (datetime('now')));
        CREATE TABLE manifests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id INTEGER NOT NULL REFERENCES input_versions(id),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            status TEXT NOT NULL CHECK (status IN ('published','failed')),
            reason TEXT);
        CREATE TABLE manifest_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            manifest_id INTEGER NOT NULL REFERENCES manifests(id),
            image_no TEXT NOT NULL, patient_no TEXT NOT NULL,
            class_name TEXT NOT NULL, component_id INTEGER NOT NULL,
            side TEXT NOT NULL CHECK (side IN ('train','val')),
            UNIQUE(manifest_id, image_no));
        CREATE TABLE partition_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id INTEGER NOT NULL REFERENCES input_versions(id),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            started_at TEXT, finished_at TEXT,
            status TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','failed')),
            fail_stage TEXT, reason TEXT, detail TEXT, manifest_id INTEGER);
        CREATE UNIQUE INDEX uq_manifest_published
            ON manifests(version_id) WHERE status='published';
        CREATE UNIQUE INDEX uq_job_active
            ON partition_jobs(version_id)
            WHERE status IN ('queued','running','succeeded');
        """
    )
    text = legacy_csv([("IMG001", "P001", "肿瘤"), ("IMG002", "P002", "肿瘤")])
    vid = conn.execute(
        "INSERT INTO input_versions(image_csv,dup_csv,image_count,class_names,quotas) "
        "VALUES (?,?,?,?,?)", (text, "", 2, "肿瘤", json.dumps({"肿瘤": 1}))
    ).lastrowid
    conn.execute("INSERT INTO images(version_id,image_no,patient_no,class_name) "
                 "VALUES (?,?,?,?)", (vid, "P001", "IMG001", "肿瘤"))
    conn.execute("INSERT INTO images(version_id,image_no,patient_no,class_name) "
                 "VALUES (?,?,?,?)", (vid, "P002", "IMG002", "肿瘤"))
    mid = conn.execute("INSERT INTO manifests(version_id,status) VALUES (?,'published')",
                       (vid,)).lastrowid
    conn.execute("INSERT INTO partition_jobs(version_id,status,manifest_id) "
                 "VALUES (?,'succeeded',?)", (vid, mid))
    conn.execute("INSERT INTO manifest_entries(manifest_id,image_no,patient_no,"
                 "class_name,component_id,side) VALUES (?,?,?,?,?,?)",
                 (mid, "P001", "IMG001", "肿瘤", 0, "val"))
    conn.execute("INSERT INTO side_registry(image_no,side,first_manifest_id) "
                 "VALUES ('P001','val',?)", (mid,))
    conn.commit()
    conn.close()

    svc = Service(db, synchronous=True)
    conn = get_db(db)
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(manifests)")}
        jcols = {r["name"] for r in conn.execute("PRAGMA table_info(partition_jobs)")}
        idx = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'")}
    finally:
        conn.close()
    assert {"quarantined"} <= cols
    assert {"voided"} <= jcols
    assert "uq_manifest_published" not in idx
    assert "uq_manifest_published_clean" in idx
    # 旧键 P001 还原为真实图像 IMG001，且旧清单被停供
    assert {r["image_no"]: r["side"] for r in svc.registry_snapshot()} == {"IMG001": "val"}
    data = read_db(db)
    assert data["manifests"][mid] == 1
    svc.close()

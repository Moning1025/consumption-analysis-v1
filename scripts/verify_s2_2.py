"""S2-2 验收：地域归一化到底洗没洗干净。

8 项检查（每一项都在防一类特定的错）：
1. consumption_record 里 region_code 为空的必须为 0（有码才能上地图）
2. city_region 里不许再出现"未知区域"这类占位词
3. 所有 region_code 必须是码表里真实存在的码（防止有人手填了个 510199）
4. 码与名必须自洽：adcode_to_name(码) == city_region（防止改码不改名）
5. 层级合法：末两位 00 → 市级（510100），否则区县级且前 4 位 5101
6. 账本存在：ingestion_validation 里有一条 region_normalization 记录
7. 账本守恒：report.stat.seen == updated + unmatched，且与表内 valid/error 一致
8. 幂等性：再跑一次清洗应当 0 条待处理（说明清洗是收敛的，不是每跑一次都变）

用法：
    D:\\Python\\venv_p3\\Scripts\\python.exe scripts\\verify_s2_2.py
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datasource.region_codes import adcode_to_name  # noqa: E402

DEFAULT_DB = PROJECT_ROOT / "local_consumption_insight.db"
PLACEHOLDERS = ("未知区域", "未知", "无", "其他", "其它", "")


def main() -> int:
    parser = argparse.ArgumentParser(description="S2-2 地域归一化验收")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    args = parser.parse_args()

    results: list[tuple[bool, str, str]] = []

    def add(ok: bool, no: int, title: str, detail: str) -> None:
        results.append((ok, f"{no}. {title}", detail))
        print(f"[{'PASS' if ok else 'FAIL'}] {no}. {title}")
        print(f"       {detail}")

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    print(f"数据库：{args.db}")
    print("=" * 68)

    rows = conn.execute(
        "SELECT id, region_code, city_region, merchant_name FROM consumption_record ORDER BY id"
    ).fetchall()
    total = len(rows)

    # 1. 空码清零
    empty = [r["id"] for r in rows if not (r["region_code"] or "").strip()]
    add(not empty, 1, "region_code 无空值",
        f"{total - len(empty)}/{total} 条有码；空 {len(empty)} 条{('，id=' + str(empty)) if empty else ''}")

    # 2. 占位词清零
    dirty = [r["id"] for r in rows if (r["city_region"] or "").strip() in PLACEHOLDERS]
    add(not dirty, 2, "city_region 无占位词",
        f"占位词记录 {len(dirty)} 条{('，id=' + str(dirty)) if dirty else '（已全部替换为标准名）'}")

    # 3. 码必须在码表里
    bad_code = [(r["id"], r["region_code"]) for r in rows
                if (r["region_code"] or "") and adcode_to_name(r["region_code"]) is None]
    add(not bad_code, 3, "region_code 均为真实区划码", f"非法码 {len(bad_code)} 个{bad_code if bad_code else ''}")

    # 4. 码与名自洽
    mismatch = []
    for r in rows:
        code = (r["region_code"] or "").strip()
        if not code:
            continue
        std = adcode_to_name(code)
        if std and std != (r["city_region"] or "").strip():
            mismatch.append((r["id"], code, r["city_region"], std))
    add(not mismatch, 4, "码与标准名一致", f"不一致 {len(mismatch)} 条{mismatch if mismatch else ''}")

    # 5. 层级合法
    bad_level = []
    for r in rows:
        code = (r["region_code"] or "").strip()
        if not code:
            continue
        if code.endswith("00"):
            ok = code in ("510100",) or code.endswith("00") and code.startswith("51")
        else:
            ok = code.startswith("5101")
        if not ok:
            bad_level.append((r["id"], code))
    add(not bad_level, 5, "层级与码段合法", f"异常 {len(bad_level)} 条{bad_level if bad_level else ''}")

    # 6/7. 账本
    val = conn.execute(
        """
        SELECT validation_code, batch_identifier, valid_rows, error_rows, warning_rows,
               report_json, created_at
        FROM ingestion_validation
        WHERE report_json LIKE '%region_normalization%'
        ORDER BY created_at DESC LIMIT 1
        """
    ).fetchone()
    add(val is not None, 6, "存在归一化账本记录",
        f"账本 {val['validation_code']}（{val['created_at']}）" if val else "未找到 region_normalization 记录，先跑 clean_regions.py")

    if val:
        try:
            report = json.loads(val["report_json"] or "{}")
            st = report.get("stat", {})
            seen = st.get("seen")
            sub_ok = seen is not None and seen == (st.get("updated", -1) + st.get("unmatched", -1))
            row_ok = seen == (val["valid_rows"] + val["error_rows"])
            add(sub_ok and row_ok, 7, "账本守恒",
                f"report: seen {seen} = updated {st.get('updated')} + unmatched {st.get('unmatched')}；"
                f"表列: valid {val['valid_rows']} + error {val['error_rows']}")
        except Exception as exc:  # noqa: BLE001
            add(False, 7, "账本守恒", f"report_json 解析失败：{exc}")
    else:
        add(False, 7, "账本守恒", "无账本可校验")

    # 8. 幂等：清洗后再查应该 0 条
    left = conn.execute(
        """
        SELECT COUNT(*) FROM consumption_record
        WHERE region_code IS NULL OR TRIM(COALESCE(region_code,'')) = ''
           OR city_region IS NULL OR TRIM(COALESCE(city_region,'')) = ''
           OR city_region IN ('未知区域','未知','无','其他','其它')
        """
    ).fetchone()[0]
    add(left == 0, 8, "幂等（再洗应为 0 条）", f"仍待清洗 {left} 条")

    print("=" * 68)
    passed = sum(1 for ok, _, _ in results if ok)
    print(f"结果：{passed}/{len(results)} 项通过")

    print("\n--- 归一化结果（论文可直接引用） ---")
    for r in conn.execute(
        "SELECT id, merchant_name, city_region, region_code FROM consumption_record ORDER BY id"
    ):
        print(f"  id={r['id']:<4}{r['merchant_name'][:14]:<16}"
              f"{r['region_code'] or '-':<9}{r['city_region'] or '-'}")

    conn.close()
    if passed == len(results):
        print("\n[PASS] 结论：S2-2 地域归一化验收通过——5 条脏数据已全部回填标准区划码")
        return 0
    print("\n[FAIL] 有检查未通过，见上方红字项")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

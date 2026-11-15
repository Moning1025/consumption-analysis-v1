"""S2-1b · 统计年鉴导入器：成都区县（xlsx） + 四川市州（伪 csv 实为 xlsx）

为什么不用"爬统计局网页"而是"导入年鉴 Excel"
--------------------------------------------
实测结论（见 docs/DATASOURCE_RESEARCH_S2_1b.md）：
- 省统计局 / 商务厅的**月度**表能拿到，但只有**省级大盘**；
- **区县级**社零没有稳定的月度公开接口（成都开放平台需登录、直连 412）；
- 而年鉴 Excel 一次给全 20 个区县 × 全部指标，是**唯一能拿到区县消费需求**的路。

所以本脚本干的是"把年鉴变成结构化数据"，跑通后再谈增量更新。
这是**方案 A：官方年鉴、手工下载、脚本解析**——答辩时必须照实说，
"数据来源是成都市/四川省统计年鉴（官方出版物），我写了导入器把 Excel 结构化入库"。

三个实测出来的坑（代码里都做了防御）
------------------------------------
1. **文件名撒谎**：`sichuan_2025_city_detailed.csv` 其实是 xlsx（文件头 `PK\\x03\\x04`）。
   按扩展名用 csv 模块打开，只会读出乱码或直接崩。→ `detect_format()` 用魔数判断，不信扩展名。
2. **汇总行混在数据里**："全 市"（中间有空格）、"四川省"。当普通地区入库，
   后面所有"区县之和 vs 全市"的校验全错。→ 用 region_codes.is_summary_row() 剔除并计数。
3. **单位不统一**：成都表是**万元**，四川表是**亿元**。→ 从表标题里解析单位，
   乘换算系数落库，并把原始单位与系数写进账本（可追溯）。

用法
----
    # 只看不写（强烈建议先跑这个）
    python scripts/stats_yearbook_importer.py --dry-run

    # 真导入（默认自动发现 data_raw 下的两个文件）
    python scripts/stats_yearbook_importer.py

    # 指定文件
    python scripts/stats_yearbook_importer.py --file data_raw/chengdu_2024_district.xlsx --file data_raw/sichuan_2025_city_detailed.csv
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datasource.region_codes import is_summary_row, lookup_adcode  # noqa: E402

DEFAULT_YEARBOOK_DIR = PROJECT_ROOT / "data_raw"

# 文件 → (地区层级, 显示名)。文件名不可靠，但"这批文件是哪个层级"仍要靠它来判断：
# 成都年鉴是区县级，四川年鉴是市州级。用**文件名里的城市/省份关键字**而不是顺序。
SOURCE_HINTS = (
    ("chengdu", "district", "成都区(市)县"),
    ("sichuan", "city", "四川市(州)"),
)

# 表头关键字 → 入库字段名（区县级与市州级共用一张映射，缺的字段就不取）
HEADER_MAP: dict[str, str] = {
    "社会消费品零售总额": "retail_total",
    "社零总额": "retail_total",
    "零售总额": "retail_total",
    "商品零售": "goods_retail",
    "餐饮收入": "catering_revenue",
    "城镇": "urban_retail",
    "乡村": "rural_retail",
    "社零增速": "retail_growth",
    "零售增速": "retail_growth",
}

NAME_HEADER_HINTS = ("区(市)县", "区（市）县", "市州", "地区", "区县")

UPSERT_SQL = """
INSERT INTO region_consumption_yearly (
    region_code, region_name, region_level, year,
    retail_total, catering_revenue, urban_retail, rural_retail,
    goods_retail, retail_growth,
    source_file, source_row_no, raw_row, imported_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(region_level, region_code, year) DO UPDATE SET
    region_name      = excluded.region_name,
    retail_total     = excluded.retail_total,
    catering_revenue = excluded.catering_revenue,
    urban_retail     = excluded.urban_retail,
    rural_retail     = excluded.rural_retail,
    goods_retail     = excluded.goods_retail,
    retail_growth    = excluded.retail_growth,
    source_file      = excluded.source_file,
    source_row_no    = excluded.source_row_no,
    raw_row          = excluded.raw_row,
    imported_at      = excluded.imported_at
"""

LOG_SQL = """
INSERT INTO yearbook_import_log (
    batch_id, source_file, file_format, sheet_name, year, region_level,
    rows_seen, rows_skipped, rows_upserted, unit_raw, unit_factor, message, created_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


# --------------------------------------------------------------------------- 数据容器
@dataclass(slots=True)
class ImportStat:
    """一次导入的统计，最后打印成一张表。

    账本口径（三个字段必须永远满足 rows_seen = rows_upserted + rows_skipped）：
    - rows_seen     ：表头以下、内容非空的数据行数（空行不计）
    - rows_skipped  ：读到但没入库的行（汇总行"全 市/四川省"、地区名未匹配、地区名为空）
    - rows_upserted ：真正写入/更新的行
    这条恒等式就是"数据绝对守恒"的账本，任何分支忘了记账，验收第 8 项立刻报不守恒。
    """

    source_file: str = ""
    file_format: str = ""
    sheet_name: str = ""
    year: int | None = None
    region_level: str = ""
    rows_seen: int = 0
    rows_skipped: int = 0
    rows_upserted: int = 0
    unit_raw: str = ""
    unit_factor: float = 1.0
    unmatched: list[str] = field(default_factory=list)
    samples: list[str] = field(default_factory=list)
    message: str = ""


# --------------------------------------------------------------------------- 工具函数
def _now_utc() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")


def detect_format(path: Path) -> tuple[str, str]:
    """用魔数判断真实格式，**不看扩展名**。

    返回 (format, note)：format ∈ {'xlsx', 'csv'}；note 用于记录"文件名撒谎"这件事。
    """
    with open(path, "rb") as fh:
        head = fh.read(8)
    if head.startswith(b"PK\x03\x04"):
        # OOXML 压缩包。再看里面有没有 xl/workbook.xml —— 有才是 xlsx，没有则可能是普通 zip
        try:
            with zipfile.ZipFile(path) as zf:
                names = set(zf.namelist())
            if "xl/workbook.xml" in names:
                note = "xlsx"
                if path.suffix.lower() in (".csv", ".txt"):
                    note = f"xlsx（文件名是 {path.suffix}，实为 Excel，已按魔数纠正）"
                return "xlsx", note
        except zipfile.BadZipFile:
            pass
        return "csv", "zip 但不像 xlsx，按文本尝试"
    # 不是 zip → 当文本处理（GBK / UTF-8 都可能）
    if path.suffix.lower() in (".xlsx", ".xls"):
        return "csv", "扩展名是 Excel 但文件头不是 zip，按文本尝试（可能已损坏）"
    return "csv", "csv"


def parse_year_from_name(path: Path) -> int | None:
    """从文件名里抓 4 位年份（如 2024 / 2025），抓不到返回 None。"""
    match = re.search(r"(19|20)\d{2}", path.stem)
    return int(match.group(0)) if match else None


def normalize_header(text: object) -> str:
    """表头归一化：去空格/括号说明，方便模糊匹配。"""
    if text is None:
        return ""
    raw = str(text).replace("\u3000", "").replace(" ", "")
    raw = raw.replace("（", "(").replace("）", ")")
    return raw


def to_float(cell: object) -> float | None:
    """把单元格转成 float；'107,950,000' / '108.35 ' / '-' 这类都能吃。"""
    if cell is None:
        return None
    if isinstance(cell, (int, float)):
        return float(cell)
    text = str(cell).strip().replace(",", "").replace("，", "").replace("%", "")
    if text in ("", "-", "—", "－", "nan", "NaN", "None"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_unit(title: str, default_factor: float) -> tuple[str, float]:
    """从标题/表头里找单位，返回 (单位原文, 转万元系数)。

    "单位：万元" → ('万元', 1)；"社零总额(亿元)" → ('亿元', 10000)。
    找不到就用 default_factor（成都默认 1，四川默认 10000，见调用处）。
    """
    text = str(title or "")
    if "亿元" in text:
        return "亿元", 10000.0
    if "万元" in text:
        return "万元", 1.0
    return ("未知", default_factor)


# --------------------------------------------------------------------------- 读表
def read_xlsx_rows(path: Path) -> list[tuple[str, list[list[object]]]]:
    """读 xlsx 的所有 sheet，返回 [(sheet名, 行列表)]。**传文件对象**而不是路径，
    因为文件可能叫 .csv（openpyxl 会按扩展名拒绝）。"""
    from openpyxl import load_workbook

    with open(path, "rb") as fh:
        wb = load_workbook(fh, read_only=True, data_only=True)
        out: list[tuple[str, list[list[object]]]] = []
        for ws in wb.worksheets:
            rows = [list(r) for r in ws.iter_rows(values_only=True)]
            out.append((ws.title, rows))
        wb.close()
    return out


def read_csv_rows(path: Path) -> list[list[str]]:
    """读文本 CSV，编码依次尝试 utf-8-sig → gbk（国内年鉴表常见 GBK）。"""
    for encoding in ("utf-8-sig", "utf-8", "gbk", "gb18030"):
        try:
            with open(path, "r", encoding=encoding, newline="") as fh:
                import csv

                return [row for row in csv.reader(fh)]
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("utf-8/gbk", b"", 0, 1, f"无法解码 {path.name}（试过 utf-8-sig/utf-8/gbk/gb18030）")


def pick_sheet(sheets: list[tuple[str, list[list[object]]]], level: str) -> tuple[str, list[list[object]]]:
    """挑出"含零售总额"的那个 sheet。

    成都年鉴：sheet 名就叫「零售总额」（还有预算收入/预算支出，不能乱取第一个）。
    找不到关键字就退化为"第一个有 ≥3 行的 sheet"，并把这件事写进日志。
    """
    for name, rows in sheets:
        if "零售" in name and rows:
            return name, rows
    for name, rows in sheets:
        flat = " ".join(normalize_header(c) for r in rows[:5] for c in r)
        if "零售总额" in flat:
            return name, rows
    for name, rows in sheets:
        if len(rows) >= 3:
            return name, rows
    return (sheets[0][0], sheets[0][1]) if sheets else ("", [])


def find_header_row(rows: list[list[object]]) -> int | None:
    """找表头行：**给每一行打分，取最高分**。

    为什么要打分（实测踩过的坑）：
    成都表的第 1 行是标题「表15-27 成都区(市)县社会消费品零售总额 (2024年，单位：万元)」，
    这行**同时包含**"区(市)县"和"零售总额"两个关键字，如果只判断"有关键字"就会把标题行当表头，
    结果整张表的数据列全对不上——而且不会报错，只会静默读出 20 行空值。

    打分规则：命中多少个"指标字段"（retail_total/goods_retail/...）+ 是否含地区列关键字；
    要求至少命中 2 个指标字段，防止标题行（只有一个字段、且是长句子）混进来。
    """
    best_idx: int | None = None
    best_score = 0
    for idx, row in enumerate(rows[:15]):          # 表头不会出现在 15 行以后
        cells = [normalize_header(c) for c in row]
        non_empty = [c for c in cells if c]
        if len(non_empty) < 3:                     # 数据行/空行先排除
            continue
        joined = "".join(cells)
        has_name = any(hint.replace(" ", "") in joined for hint in NAME_HEADER_HINTS)

        fields: set[str] = set()
        for cell in cells:
            if not cell or len(cell) > 20:         # 长句子是标题，不是列名
                continue
            for key, field_name in HEADER_MAP.items():
                if key in cell:
                    fields.add(field_name)
                    break

        if not has_name or len(fields) < 2:
            continue
        score = len(fields) + 1
        if score > best_score:
            best_idx, best_score = idx, score
    return best_idx


def map_columns(header_row: list[object]) -> tuple[int | None, dict[str, int]]:
    """把表头映射成 {字段名: 列号}，以及名称列列号。"""
    name_col: int | None = None
    value_cols: dict[str, int] = {}
    for idx, cell in enumerate(header_row):
        text = normalize_header(cell)
        if not text:
            continue
        if name_col is None and any(hint.replace(" ", "") in text for hint in NAME_HEADER_HINTS):
            name_col = idx
            continue
        # 精确优先，再退化成包含匹配（"社零总额(亿元)" 里含 "社零总额"）
        for key, field_name in HEADER_MAP.items():
            if text == key or key in text:
                # 同一字段只取第一次出现（避免"增速"列覆盖"总额"列）
                value_cols.setdefault(field_name, idx)
                break
    if name_col is None:
        name_col = 0  # 兜底：第一列就是地区名
    return name_col, value_cols


# --------------------------------------------------------------------------- 主流程
def import_file(
    conn: sqlite3.Connection,
    path: Path,
    level: str,
    year: int | None,
    dry_run: bool,
    default_factor: float,
    batch_id: str,
) -> ImportStat:
    """导入一个文件，返回统计。dry_run=True 时只统计不写库。"""
    stat = ImportStat(source_file=path.name, region_level=level)
    fmt, note = detect_format(path)
    stat.file_format = note

    if fmt == "xlsx":
        sheets = read_xlsx_rows(path)
        sheet_name, rows = pick_sheet(sheets, level)
        stat.sheet_name = sheet_name
    else:
        rows = read_csv_rows(path)
        stat.sheet_name = "-"

    if not rows:
        stat.message = "文件里没读到任何行"
        return stat

    header_idx = find_header_row(rows)
    if header_idx is None:
        stat.message = "找不到表头（没有同时出现地区列与零售指标列）"
        return stat

    name_col, value_cols = map_columns(rows[header_idx])

    # 单位：先看表头上方 1~2 行（成都表把"单位：万元"和年份写在标题里），再看表头本身
    title_text = " ".join(
        normalize_header(c) for r in rows[: header_idx + 1] for c in r if c is not None
    )
    unit_raw, factor = parse_unit(title_text, default_factor)
    stat.unit_raw, stat.unit_factor = unit_raw, factor

    # 年份：优先命令行/文件名；都没有再看标题里的 "2024年"
    if year is None:
        year = parse_year_from_name(path)
    if year is None:
        match = re.search(r"(19|20)\d{2}\s*年", title_text)
        if match:
            year = int(match.group(0)[:4])
    stat.year = year
    if year is None:
        stat.message = "无法确定年份（文件名和标题里都没有 4 位年份）"
        return stat

    now = _now_utc()
    for row_idx in range(header_idx + 1, len(rows)):
        row = rows[row_idx]
        if not row or all(c is None or str(c).strip() == "" for c in row):
            continue  # 空行：整行都空，既不算"读到"也不算"剔除"
        # 账本口径：从这一行起就是"读到的数据行"，后面无论入库还是被剔除都要记账，
        # 保证恒等式 rows_seen = rows_upserted + rows_skipped 在任何分支下都成立。
        stat.rows_seen += 1
        raw_name = row[name_col] if name_col < len(row) else None
        if raw_name is None or str(raw_name).strip() == "":
            stat.rows_skipped += 1
            continue
        if is_summary_row(str(raw_name)):
            stat.rows_skipped += 1      # 全市 / 四川省：剔除并计数（不是丢弃不报）
            continue

        code, std_name = lookup_adcode(str(raw_name), level)
        if code is None:
            stat.rows_skipped += 1
            stat.unmatched.append(str(raw_name).strip())
            continue

        values: dict[str, float | None] = {}
        for field_name, col in value_cols.items():
            cell = row[col] if col < len(row) else None
            number = to_float(cell)
            if number is not None and field_name != "retail_growth":
                number = number * factor          # 只有金额要换算，增速是比例不能乘
            values[field_name] = number

        if dry_run:
            stat.rows_upserted += 1              # dry-run 里"会写多少行"
            if len(stat.samples) < 3:            # 抽 3 行打印，让人肉眼确认列没串
                stat.samples.append(
                    f"{std_name}({code}) "
                    + " ".join(
                        f"{k}={'空' if v is None else f'{v:,.1f}'}"
                        for k, v in values.items()
                    )
                )
            continue

        conn.execute(
            UPSERT_SQL,
            (
                code, std_name, level, year,
                values.get("retail_total"), values.get("catering_revenue"),
                values.get("urban_retail"), values.get("rural_retail"),
                values.get("goods_retail"), values.get("retail_growth"),
                path.name, row_idx + 1,
                json.dumps([None if c is None else str(c) for c in row], ensure_ascii=False),
                now,
            ),
        )
        stat.rows_upserted += 1

    if stat.unmatched:
        stat.message = "未匹配区划：" + "、".join(stat.unmatched)
    return stat


def resolve_level(path: Path) -> tuple[str, float]:
    """根据文件名判断层级与默认换算系数。"""
    stem = path.name.lower()
    for keyword, level, _label in SOURCE_HINTS:
        if keyword in stem:
            return level, (1.0 if level == "district" else 10000.0)
    # 兜底：名字里没有关键字，按"文件在某地上级目录"猜不了，就按区县处理并要求人工确认
    return "district", 1.0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="S2-1b 统计年鉴导入器（成都区县 xlsx / 四川市州 伪 csv）")
    parser.add_argument("--file", action="append", default=None, help="年鉴文件路径，可多次指定；缺省自动发现")
    parser.add_argument("--db", default=None, help="数据库路径（缺省用项目默认库）")
    parser.add_argument("--year", type=int, default=None, help="强制指定年份（缺省从文件名解析）")
    parser.add_argument("--dry-run", action="store_true", help="只解析与统计，不写库")
    args = parser.parse_args(argv)

    from store.db_connection import create_connection, initialize_schema

    db_path = Path(args.db) if args.db else PROJECT_ROOT / "local_consumption_insight.db"
    if args.dry_run:
        conn = sqlite3.connect(":memory:")       # dry-run 用内存库，绝不碰真实数据
        conn.executescript((PROJECT_ROOT / "schema_v4.sql").read_text(encoding="utf-8"))
    else:
        initialize_schema()
        conn = create_connection(db_path)

    files: list[Path] = [Path(p) for p in args.file] if args.file else []
    if not files:
        for keyword, _level, _label in SOURCE_HINTS:
            found = sorted(DEFAULT_YEARBOOK_DIR.glob(f"{keyword}*"))
            found = [p for p in found if p.suffix.lower() in (".xlsx", ".xls", ".csv", ".txt")]
            if found:
                files.append(found[0])
    if not files:
        print(f"[FAIL] 在 {DEFAULT_YEARBOOK_DIR} 下没有找到年鉴文件（chengdu* / sichuan*）")
        return 2

    batch_id = "yb_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    print("=" * 72)
    print(f"数据库：{db_path}")
    print(f"批次  ：{batch_id}{'   [dry-run 不写库]' if args.dry_run else ''}")
    print(f"文件  ：{', '.join(p.name for p in files)}")
    print("=" * 72)

    stats: list[ImportStat] = []
    for path in files:
        if not path.exists():
            print(f"[WARN] 文件不存在，跳过：{path}")
            continue
        level, default_factor = resolve_level(path)
        print(f"\n>>> {path.name}  （判定层级：{level}，默认单位系数 {default_factor:g}）")
        stat = import_file(conn, path, level, args.year, args.dry_run, default_factor, batch_id)
        stats.append(stat)

        print(f"    格式      : {stat.file_format}")
        print(f"    sheet     : {stat.sheet_name}")
        print(f"    年份/层级 : {stat.year} / {stat.region_level}")
        print(f"    单位      : {stat.unit_raw} （×{stat.unit_factor:g} 转万元）")
        print(f"    读到/剔除/入库 : {stat.rows_seen} / {stat.rows_skipped} / {stat.rows_upserted}")
        print(f"    账本核对  : {stat.rows_seen} = {stat.rows_upserted} + {stat.rows_skipped}"
              f"  → {'守恒' if stat.rows_seen == stat.rows_upserted + stat.rows_skipped else '不守恒 ⚠'}")
        if stat.message:
            print(f"    备注      : {stat.message}")
        if stat.samples:
            print("    抽样 3 行 :")
            for line in stat.samples:
                print(f"      · {line}")

        if not args.dry_run:
            conn.execute(
                LOG_SQL,
                (
                    batch_id, stat.source_file, stat.file_format, stat.sheet_name,
                    stat.year, stat.region_level, stat.rows_seen, stat.rows_skipped,
                    stat.rows_upserted, stat.unit_raw, stat.unit_factor, stat.message, _now_utc(),
                ),
            )

    if not args.dry_run:
        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM region_consumption_yearly").fetchone()[0]
        by_level = conn.execute(
            "SELECT region_level, year, COUNT(*) FROM region_consumption_yearly GROUP BY 1,2 ORDER BY 1,2"
        ).fetchall()
        print("\n" + "=" * 72)
        print(f"region_consumption_yearly 现有 {total} 行")
        for level, year, count in by_level:
            print(f"  {level:<9} {year} 年  {count} 个地区")
        print("下一步：D:\\Python\\venv_p3\\Scripts\\python.exe scripts\\verify_s2_1b.py")
    else:
        print("\n[dry-run] 未写库。去掉 --dry-run 再跑一次才会真正落盘。")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

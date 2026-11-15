"""S2-2 地域归一化：把五花八门的地名文本压成标准行政区划码。

【为什么需要它】
`consumption_record` 里 `city_region` 全是"未知区域"、`region_code` 全 NULL，
这样的数据没法上地图、没法按区县聚合、没法跟 POI/人口/灯光做空间 join。
归一化就是给每一条消费记录"编目"——图书馆给书贴索书号，快递给包裹贴分拣码，
本质都是：**把人类随手写的文本，映射到一个唯一的数字编号上**。

【四路匹配，优先级从高到低】
1. exact     精确命中码表            "武侯区"  → 510107        置信度 1.00
2. alias     别名/俗称命中           "高新西区" → 510107（非行政区，托管街道归武侯）
3. landmark  地标/门店名里扫地名     "成都春熙路店" → 510104 锦江区   置信度 0.95
4. fuzzy     编辑距离模糊匹配        "武候区"（错别字）→ 510107        置信度 0.75~0.9
都没有 → unmatched，**不猜、不塞默认值**，交由人工复核。

【关键设计：为什么不直接给个默认区域】
乱填一个"成都市"看着漂亮，但会让所有空间分析失真——这是数据造假的起点。
宁可留空报错，也不制造假数据。这是本项目全程的口径。

【分层标注 · 按工作区规则 v2】
- 本文件 = 第二层：沫宁鱼写完整版 + 逐行中文注释，老大必须改一处（见文件末尾）
"""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass, field

from datasource.region_codes import (
    CHENGDU_DISTRICTS,
    SICHUAN_CITIES,
    adcode_to_name,
    normalize_region_name,
)

# ======================================================================
# 常量区
# ======================================================================

# 这些词一眼就知道"这条数据没写地名"，直接判定 unmatched，不浪费算力去模糊匹配。
UNKNOWN_TOKENS: frozenset[str] = frozenset(
    {"未知区域", "未知", "无", "none", "null", "nan", "-", "—", "其它", "其他", "不详", ""}
)

# 地标/商圈/俗称 → (区划码, 标准名, 判定理由)
# 这些是"码表里查不到、但成都人都知道在哪"的写法。
# 注意：高新西区、天府新区**不是行政区划**，是按托管街道归并的近似映射，
# 论文和答辩里必须写明这一点，否则等于伪造行政归属。
LANDMARKS: dict[str, tuple[str, str, str]] = {
    "春熙路": ("510104", "锦江区", "商圈，位于锦江区"),
    "太古里": ("510104", "锦江区", "商圈，位于锦江区"),
    "天府广场": ("510105", "青羊区", "地标，位于青羊区"),
    "宽窄巷子": ("510105", "青羊区", "景区，位于青羊区"),
    "武侯祠": ("510107", "武侯区", "景区，位于武侯区"),
    "锦里": ("510107", "武侯区", "景区，位于武侯区"),
    "环球中心": ("510107", "武侯区", "综合体，位于武侯区"),
    "高新西区": ("510107", "武侯区", "非行政区，高新区托管街道近似归武侯区"),
    "高新区": ("510107", "武侯区", "非行政区，近似归武侯区"),
    "天府新区": ("510100", "成都市", "跨区新区，只能上推到市级"),
    "双流机场": ("510116", "双流区", "机场，位于双流区"),
    "金融城": ("510104", "锦江区", "商圈，位于锦江区"),
}

# 模糊匹配的相似度阈值：0.65 是经验值——中文短名（3 个字）差一个字相似度就掉到 0.67，
# 阈值设 0.75 会把"武候区→武侯区"这种常见错别字拒之门外。低于 0.6 又开始乱认。
_FUZZY_CUTOFF = 0.65

# 是否启用"从门店名里扫地标"这条兜底路。老大必改一处就改这里或上面的阈值。
USE_LANDMARK_FALLBACK = True

# 以"市/区/县/州/镇/街道"结尾但这些并不是区县的常见噪声（避免误命中）
_NOISE_SUFFIX = ("街道", "镇", "乡")


# ======================================================================
# 结果对象
# ======================================================================


@dataclass
class NormalizeResult:
    """一次归一化的完整"证据链"。

    为什么要有 method 和 confidence：验收时能回答"这条码是怎么来的"。
    只有 code 没有 method，等于考试只给答案不给过程。
    """

    code: str | None = None          # 6 位行政区划码，None 表示没匹配上
    name: str | None = None          # 标准名（码表里的写法）
    level: str | None = None         # 'city' 市级 / 'district' 区县级
    method: str = "unmatched"        # exact / alias / landmark / fuzzy / unmatched
    confidence: float = 0.0          # 0.0~1.0
    note: str = ""                   # 人话解释，写进报告给老师看
    candidates: list[str] = field(default_factory=list)  # 模糊匹配的候选，便于人工复核

    @property
    def ok(self) -> bool:
        return self.code is not None


# ======================================================================
# 工具函数
# ======================================================================


def _level_of(code: str) -> str:
    """判断一个 6 位码是市级还是区县级。

    规则：标准行政区划码里，市辖区/县是 5101XX，市级是 510100 这种末两位 00。
    （注意：这是本项目够用的简化，不是国标全集的完备实现。）
    """
    return "city" if code.endswith("00") else "district"


def _clean(text: object) -> str:
    """把原始文本压成可比对的形式：去空白、去全角空格、转字符串。"""
    if text is None:
        return ""
    s = str(text).replace("\u3000", " ")
    s = re.sub(r"\s+", "", s)
    return s


def _name_tables() -> list[tuple[str, str]]:
    """把两张码表拼成 [(名字, 码)] 的扁平列表，省得后面到处写两遍循环。"""
    rows: list[tuple[str, str]] = []
    for table in (CHENGDU_DISTRICTS, SICHUAN_CITIES):
        for name, code in table.items():
            rows.append((name, code))
    return rows


# ======================================================================
# 主入口：文本 → 标准码
# ======================================================================


def normalize_region(raw: object, fuzzy_cutoff: float = _FUZZY_CUTOFF) -> NormalizeResult:
    """把一个地名文本归一化成标准区划码。

    参数
    ----
    raw          : 任意文本，"成都市武侯区"、"武候区"、"未知区域" 都能喂进来
    fuzzy_cutoff : 模糊匹配阈值，越低越敢猜（也越容易猜错）

    返回
    ----
    NormalizeResult：一定不抛异常，没匹配上就是 ok=False
    """
    text = _clean(raw)

    # ---- 第 0 步：空值/占位词，直接判死，别让模糊匹配去瞎猜 ----
    if not text or text.lower() in UNKNOWN_TOKENS:
        return NormalizeResult(note="原文为空或占位词")

    # ---- 第 1 步：精确命中区县表（最简单也最可靠）----
    norm = normalize_region_name(text)                  # 复用 S2-1b 的压缩规则（去空白+展开简称）
    if norm in CHENGDU_DISTRICTS:
        code = CHENGDU_DISTRICTS[norm]
        return NormalizeResult(code, norm, _level_of(code), "exact", 1.0, "区县表精确命中")

    # ---- 第 2 步：精确命中市州表 ----
    if norm in SICHUAN_CITIES:
        code = SICHUAN_CITIES[norm]
        return NormalizeResult(code, norm, _level_of(code), "exact", 1.0, "市州表精确命中")

    # ---- 第 3 步：补后缀命中（"成都" → "成都市"，码表键都带后缀）----
    # 这一步必须排在模糊匹配之前：能精确补出来就别去算相似度，省算力也更可靠。
    for suffix in ("市", "区", "县", "州"):
        candidate = norm + suffix
        if candidate in CHENGDU_DISTRICTS:
            code = CHENGDU_DISTRICTS[candidate]
            return NormalizeResult(code, candidate, _level_of(code), "exact", 1.0,
                                   f"补后缀「{suffix}」命中区县表")
        if candidate in SICHUAN_CITIES:
            code = SICHUAN_CITIES[candidate]
            return NormalizeResult(code, candidate, _level_of(code), "exact", 1.0,
                                   f"补后缀「{suffix}」命中市州表")

    # ---- 第 4 步：别名/地标表（"高新西区"这种码表里根本没有的写法）----
    if norm in LANDMARKS:
        code, name, why = LANDMARKS[norm]
        return NormalizeResult(code, name, _level_of(code), "alias", 0.95, why)

    # ---- 第 5 步：地标作为子串出现（"成都春熙路店" 里含 "春熙路"）----
    # 注意这里按 alias 长度倒序扫描：长词优先，避免 "春熙" 抢先命中 "春熙路"。
    for alias in sorted(LANDMARKS, key=len, reverse=True):
        if alias in norm:
            code, name, why = LANDMARKS[alias]
            return NormalizeResult(
                code, name, _level_of(code), "landmark", 0.90,
                f"文本内含地标「{alias}」（{why}）",
            )

    # ---- 第 6 步：带前缀的完整写法（"成都市武侯区"、"四川省成都市"）----
    # 按名字长度倒序，保证 "都江堰市" 不被 "江堰" 之类先截胡（表里键都是全名，安全）。
    for name, code in sorted(_name_tables(), key=lambda kv: len(kv[0]), reverse=True):
        if name in norm:
            return NormalizeResult(
                code, name, _level_of(code), "alias", 0.95, f"文本内含标准地名「{name}」"
            )

    # ---- 第 7 步：模糊匹配（错别字、简称、多字少字）----
    names = [n for n, _ in _name_tables()]
    close = difflib.get_close_matches(norm, names, n=3, cutoff=fuzzy_cutoff)
    if close:
        best = close[0]
        ratio = round(difflib.SequenceMatcher(None, norm, best).ratio(), 3)
        code = dict(_name_tables())[best]
        return NormalizeResult(
            code, best, _level_of(code), "fuzzy", ratio,
            f"模糊匹配（相似度 {ratio}），原文不标准，建议人工复核",
            candidates=close,
        )

    # ---- 兜底：认不出来就是认不出来 ----
    return NormalizeResult(note=f"码表与地标表均未命中（原文：{text}）")


# ======================================================================
# 第二入口：从门店名里挖地名（本项目 5 条脏数据的真正救星）
# ======================================================================


def extract_from_merchant(merchant_name: object) -> NormalizeResult:
    """从门店名里抽地名。

    【为什么需要】实测 `consumption_record` 的原始 CSV 里**根本没有地区列**，
    唯一能推断位置的信息是 "成都春熙路店"、"太古里店"、"天府广场店" 这样的门店名。
    所以真正的归一化不是"清洗 city_region 字段"（它本来就是空的），
    而是**做一次微型的地理实体识别（gazetteer matching）**。

    这跟高德 POI 的 keyword 检索是同一个思路，只是语料小得多。
    """
    if not USE_LANDMARK_FALLBACK:
        return NormalizeResult(note="地标兜底已关闭（USE_LANDMARK_FALLBACK=False）")
    text = _clean(merchant_name)
    if not text:
        return NormalizeResult(note="门店名为空")
    return normalize_region(text)


def resolve_record(city_region: object, merchant_name: object) -> NormalizeResult:
    """一条消费记录的归一化总策略：先看地域字段，再退到门店名。

    顺序不可颠倒：字段里写了地名就以字段为准（准确度高），
    字段是"未知区域"才去门店名里找线索（是推断，置信度要打折）。
    """
    first = normalize_region(city_region)
    if first.ok:
        return first

    second = extract_from_merchant(merchant_name)
    if second.ok:
        # 置信度打个折，并在 note 里标明"这是推断出来的"，报告里一眼能区分
        second.confidence = round(second.confidence * 0.9, 3)
        second.note = f"[由门店名推断] {second.note}"
        return second

    return NormalizeResult(note=f"地域字段与门店名均未命中（{city_region!r} / {merchant_name!r}）")


# ======================================================================
# 自测：`python -m datasource.region_normalizer`
# ======================================================================

if __name__ == "__main__":
    cases = [
        ("武侯区", "区县精确"),
        ("成都市", "市级精确"),
        ("成都", "缺后缀"),
        ("高新西区", "别名/非行政区"),
        ("成都市武侯区", "带前缀的完整写法"),
        ("武候区", "错别字→模糊匹配"),
        ("未知区域", "占位词"),
        ("", "空值"),
        ("凉山州", "市州简称"),
        ("春熙路商圈", "地标子串"),
    ]
    print(f"{'原文':<12}{'码':<8}{'标准名':<8}{'层级':<10}{'方式':<10}{'置信':<7}说明")
    print("-" * 92)
    for raw, why in cases:
        r = normalize_region(raw)
        print(f"{raw or '(空)':<12}{r.code or '-':<8}{r.name or '-':<8}{r.level or '-':<10}"
              f"{r.method:<10}{r.confidence:<7.2f}{r.note}  ← {why}")

    print("\n-- 门店名推断 --")
    for m in ("成都春熙路店", "太古里店", "天府广场店", "春熙路店", "某某便利店"):
        r = extract_from_merchant(m)
        print(f"{m:<14}→ {r.code or '未命中':<8}{r.name or '-':<8}{r.method:<10}{r.note}")

    # 反查自检：码 → 名 必须能转回来，否则说明码表里有重复码
    bad = [c for c in ("510104", "510107", "510100") if adcode_to_name(c) is None]
    print("\n反查自检：", "通过" if not bad else f"失败 {bad}")


# ======================================================================
# 【老大必改一处】二选一，改完这轮才算结束
# ======================================================================
# 选项 A（最简单，1 行）：把本文件上方的 `USE_LANDMARK_FALLBACK = True` 改成 False，
#   然后重跑 clean_regions.py --dry-run，观察"未命中"从 0 变成 5——
#   你会亲眼看到那 5 条脏数据**全靠门店名兜底才救回来**的。
#
# 选项 B（进阶，1 行）：把 `_FUZZY_CUTOFF` 从 0.65 改成 0.90，
#   然后重跑 `python -m datasource.region_normalizer`，看 "武候区"（错别字）
#   从 fuzzy/0.667 变成 unmatched——理解"阈值越高越保守、越低越敢猜"的取舍。
#
# 改完把两次 dry-run 的数字贴回来，我们据此写论文"测试"章节。

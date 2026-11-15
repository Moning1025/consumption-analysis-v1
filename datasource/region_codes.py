"""行政区划码表 + 名称归一化（第一层：纯查表，我写完整版，老大只读）。

为什么需要这张表
----------------
年鉴里的地区名是"人话"（"武侯区""凉山州""全 市"），数据库里要的是**行政区划码**
（GB/T 2260，如武侯区 510107）。后面做空间融合时，POI 的 adcode 也是这个码，
两边的码对齐了才能把"供给（POI）"和"需求（社零）"拼到同一个区县上。

三个必须防的坑（都是实测出来的）
--------------------------------
1. 汇总行带空格："全 市"（中间一个全角/半角空格）、"四川省"——必须去空格后比对，否则会被当成一个区县入库。
2. 后缀不统一：年鉴写"成都"，行政码表叫"成都市"；写"凉山州"，全称是"凉山彝族自治州"。
   所以查表前统一做 normalize：去掉所有空白 + 去掉尾部"市/州/地区"。
3. 非标准区划：成都的"高新区""天府新区"不是正式区县（没有独立 6 位码），年鉴里也不会单列，
   遇到就返回 None，由调用方决定跳过还是留原名（**不许硬编码瞎猜码**）。

码表来源：GB/T 2260 中华人民共和国行政区划代码（成都 20 个区县 + 四川 21 个市州）。
这份表是静态的，行政区划调整时手工改这里即可——只有 41 行，比维护抓取脚本便宜得多。
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------- 成都 20 个区（市）县
# 顺序与年鉴表一致：5 个主城区 → 6 个近郊 → 新津 → 简阳 → 4 个县级市 → 3 个县
CHENGDU_DISTRICTS: dict[str, str] = {
    "锦江区": "510104",
    "青羊区": "510105",
    "金牛区": "510106",
    "武侯区": "510107",
    "成华区": "510108",
    "龙泉驿区": "510112",
    "青白江区": "510113",
    "新都区": "510114",
    "温江区": "510115",
    "双流区": "510116",
    "郫都区": "510117",
    "新津区": "510118",
    "简阳市": "510185",
    "都江堰市": "510181",
    "彭州市": "510182",
    "邛崃市": "510183",
    "崇州市": "510184",
    "金堂县": "510121",
    "大邑县": "510129",
    "蒲江县": "510131",
}

# ---------------------------------------------------------------- 四川 21 个市（州）
SICHUAN_CITIES: dict[str, str] = {
    "成都市": "510100",
    "自贡市": "510300",
    "攀枝花市": "510400",
    "泸州市": "510500",
    "德阳市": "510600",
    "绵阳市": "510700",
    "广元市": "510800",
    "遂宁市": "510900",
    "内江市": "511000",
    "乐山市": "511100",
    "南充市": "511300",
    "眉山市": "511400",
    "宜宾市": "511500",
    "广安市": "511600",
    "达州市": "511700",
    "雅安市": "511800",
    "巴中市": "511900",
    "资阳市": "512000",
    "阿坝藏族羌族自治州": "513200",
    "甘孜藏族自治州": "513300",
    "凉山彝族自治州": "513400",
}

# 汇总行：出现这些名字就剔除（去空格后比对），它们不是"地区"，是"合计"
SUMMARY_NAMES: frozenset[str] = frozenset({"全市", "全省", "四川省", "成都市", "合计", "总计", "全省合计"})

# =============================================================================
# 市州级"近似外包框"（bbox）：用于 S2-1d 生成空间网格时的区域限制
# -----------------------------------------------------------------------------
# 格式：(经度最小, 纬度最小, 经度最大, 纬度最大)，WGS-84，单位"度"。
# 注意：这是**矩形外包框**，不是行政边界——矩形里必然含少量相邻市县的地块。
#       论文里要如实写"外包框网格"而不是"行政区内网格"，否则面积对账会被问穿。
# 新增一个市州 = 在这里加一行，然后 --city 眉山市 就能一键复用整套流程。
# 取值方法：在地图上量该市最西/最南/最东/最北的点，向外留 0.05~0.1 度余量。
# =============================================================================
SICHUAN_CITY_BBOXES: dict[str, tuple[float, float, float, float]] = {
    "成都市": (102.5, 29.0, 105.0, 31.5),      # 成都全域（含简阳、彭州、都江堰等远郊）
    "眉山市": (102.8, 29.5, 104.5, 30.5),      # 预留：东坡区/彭山/仁寿/洪雅/丹棱/青神
    "广安市": (105.5, 30.0, 107.2, 31.0),      # 预留：广安区/前锋/华蓥/岳池/武胜/邻水
    # "德阳市": (103.5, 30.5, 105.0, 31.6),
    # "绵阳市": (103.5, 31.0, 105.5, 33.0),
}


def city_bbox(city_name: str) -> tuple[float, float, float, float] | None:
    """按市州名查近似外包框；查不到返回 None（调用方负责报错提示）。"""
    name = normalize_region_name(city_name)
    if not name.endswith(("市", "州")) and not name.endswith("自治州"):
        name = name + "市" if name in {"成都", "眉山", "广安"} else name
    # 先按原名直查，再按简称展开后的全称查
    if name in SICHUAN_CITY_BBOXES:
        return SICHUAN_CITY_BBOXES[name]
    full = _ALIASES.get(name)
    if full and full in SICHUAN_CITY_BBOXES:
        return SICHUAN_CITY_BBOXES[full]
    return None


def city_adcode(city_name: str) -> str | None:
    """市州名 → 6 位行政区划码（写进 grid_metrics.city_code 用）。"""
    name = normalize_region_name(city_name)
    if name in SICHUAN_CITIES:
        return SICHUAN_CITIES[name]
    full = _ALIASES.get(name)
    if full and full in SICHUAN_CITIES:
        return SICHUAN_CITIES[full]
    code, _ = lookup_adcode(name, "city")
    return code

# 简称 → 全称（年鉴里常写简称）
_ALIASES: dict[str, str] = {
    "凉山州": "凉山彝族自治州",
    "凉山": "凉山彝族自治州",
    "阿坝州": "阿坝藏族羌族自治州",
    "阿坝": "阿坝藏族羌族自治州",
    "甘孜州": "甘孜藏族自治州",
    "甘孜": "甘孜藏族自治州",
}

_WS_RE = re.compile(r"\s+")  # 含全角空格 \u3000？


def normalize_region_name(raw: str) -> str:
    """把年鉴里的地区名压成可比对的形式。

    规则：去掉**所有**空白（含全角空格）→ 展开简称。**不**去后缀，
    因为"都江堰市"和"锦江区"在同一张表里，去后缀会撞名。
    """
    if raw is None:
        return ""
    text = str(raw).replace("\u3000", " ")          # 全角空格先变半角空格
    text = _WS_RE.sub("", text)                      # 再去掉所有空白
    return _ALIASES.get(text, text)


def is_summary_row(raw_name: str) -> bool:
    """判断这一行是不是"全市/全省"这类汇总行。"""
    return normalize_region_name(raw_name) in SUMMARY_NAMES


def lookup_adcode(raw_name: str, level: str) -> tuple[str | None, str | None]:
    """按名称查行政区划码。

    参数
    ----
    raw_name : 年鉴原文里的地区名
    level    : 'district'（区县，查成都 20 个区县）或 'city'（市州，查四川 21 个市州）

    返回
    ----
    (adcode, 标准名)；查不到返回 (None, None)。查不到**不是错误**，
    调用方应当把它计入"未匹配"并打印出来——这比悄悄塞一个错码好一万倍。
    """
    name = normalize_region_name(raw_name)
    if not name:
        return None, None

    table = CHENGDU_DISTRICTS if level == "district" else SICHUAN_CITIES

    # 1) 精确命中
    if name in table:
        return table[name], name

    # 2) 补后缀命中：年鉴写"成都"，表里是"成都市"
    for suffix in ("市", "区", "县", "州"):
        candidate = name + suffix
        if candidate in table:
            return table[candidate], candidate

    # 3) 去后缀命中：年鉴写"成都市"，表里"成都"（本表键都带后缀，兜底用）
    for suffix in ("市", "区", "县"):
        if name.endswith(suffix):
            stripped = name[: -len(suffix)]
            if stripped in table:
                return table[stripped], stripped
            for suffix2 in ("市", "区", "县", "州"):
                if stripped + suffix2 in table:
                    return table[stripped + suffix2], stripped + suffix2

    return None, None


def adcode_to_name(adcode: str) -> str | None:
    """反向查：给个 6 位码，返回标准名（给 S2-2 地域归一化用）。"""
    for table in (CHENGDU_DISTRICTS, SICHUAN_CITIES):
        for name, code in table.items():
            if code == adcode:
                return name
    return None


if __name__ == "__main__":  # 自测：不连库、不联网，直接 print 结果
    samples = [
        ("武侯区", "district"), ("全 市", "district"), ("成都", "city"),
        ("凉山州", "city"), ("阿坝州", "city"), ("高新区", "district"),
        ("锦江区", "district"), ("四川省", "city"),
    ]
    for raw, lvl in samples:
        code, std = lookup_adcode(raw, lvl)
        print(f"{raw!r:<12} level={lvl:<9} → code={code} name={std} summary={is_summary_row(raw)}")

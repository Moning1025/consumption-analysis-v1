"""百度地图（BD-09）适配器 —— **框架预留版**，AK 未批之前不联网、不跑数据。

现状（2026-11-15）
-----------------
- 高德 Key 已配好（`AMAP_WEB_KEY`），S2-1a 已跑通 100 条真实 POI。
- 百度 AK **还在审批中**，所以这里只落骨架：
  - `bd09_to_wgs84()` / `wgs84_to_bd09()` 是**可以直接用的**（纯数学，不需要网络）；
  - `BaiduClient` 的请求方法保留但默认 `enabled=False`，没 AK 直接抛清晰错误，
    **绝不静默退化成假数据**。

为什么还要接百度（答辩口径）
---------------------------
不是为了"数据更多"，而是为了**交叉校验覆盖率**：同一个商圈，高德有 40 家店、百度有 60 家，
差的那 20 家很可能是"高德没收录的小店"。两份独立数据源比一遍，能得出
"我们的 POI 覆盖率大致在什么量级"——这是数据质量章节里最硬的一段论证。
表里已经预留 `grid_metrics.poi_density_baidu`，等 AK 下来只补一个采集脚本，不用改表结构。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from datasource.coord_transform import bd09_to_wgs84, wgs84_to_bd09

BD_PLACE_SEARCH_URL = "https://api.map.baidu.com/place/v2/search"

# AK 环境变量候选名（跟高德一样，允许几种常见写法，谁先有值用谁）
AK_ENV_CANDIDATES = ("BAIDU_MAP_AK", "BAIDU_AK", "BAIDU_MAP_KEY", "BD_AK")


def load_baidu_ak() -> str | None:
    """按顺序找百度 AK；找不到返回 None（调用方据此决定是否降级，不要写空字符串假装有）。"""
    for name in AK_ENV_CANDIDATES:
        value = os.environ.get(name)
        if value and value.strip():
            return value.strip()
    return None


@dataclass(slots=True)
class BaiduPoi:
    """一条百度 POI 的归一化结果（字段对齐 poi_merchant，方便将来直接合并）。"""

    poi_id: str
    name: str
    lng: float          # 已转成 WGS-84
    lat: float          # 已转成 WGS-84
    lng_bd09: float     # 原始 BD-09（留档，便于反查）
    lat_bd09: float
    address: str
    province: str
    city: str
    district: str
    category: str
    tel: str


def parse_baidu_poi(item: dict[str, Any]) -> BaiduPoi | None:
    """把百度 place/v2/search 的一条结果转成 BaiduPoi；坐标缺失返回 None。

    百度返回结构：{"name":..., "location": {"lat": 30.6, "lng": 104.0}, ...}
    注意：百度的 lat/lng 是**数字**，高德是 "lng,lat" **字符串**——两个平台不一样，
    这就是为什么每个平台要自己的 adapter，而不是写一个"通用解析器"。
    """
    location = item.get("location") or {}
    try:
        lng_bd = float(location["lng"])
        lat_bd = float(location["lat"])
    except (KeyError, TypeError, ValueError):
        return None

    lng, lat = bd09_to_wgs84(lng_bd, lat_bd)
    detail = item.get("detail_info") or {}
    return BaiduPoi(
        poi_id=str(item.get("uid") or ""),
        name=str(item.get("name") or ""),
        lng=lng,
        lat=lat,
        lng_bd09=lng_bd,
        lat_bd09=lat_bd,
        address=str(item.get("address") or ""),
        province=str(item.get("province") or ""),
        city=str(item.get("city") or ""),
        district=str(item.get("area") or ""),
        category=str(detail.get("tag") or detail.get("type") or ""),
        tel=str(detail.get("phone") or item.get("telephone") or ""),
    )


class BaiduClient:
    """百度地点检索客户端（预留）。AK 未到位时构造即报错，避免"以为是空数据"。"""

    def __init__(self, ak: str | None = None, timeout: float = 5.0) -> None:
        self.ak = ak or load_baidu_ak()
        self.timeout = timeout
        if not self.ak:
            raise RuntimeError(
                "百度 AK 未配置：请先设置环境变量 "
                + " / ".join(AK_ENV_CANDIDATES)
                + "（审批通过后 setx 设置，再重开终端）。此适配器当前不联网。"
            )

    def search_region(self, query: str, region: str, page_num: int = 0, page_size: int = 20) -> list[BaiduPoi]:
        """按行政区检索 POI。**未实现网络请求**——AK 到位后按下面 TODO 补。"""
        raise NotImplementedError(
            "AK 审批通过后实现：GET {url}?query={q}&region={r}&output=json&page_num=&page_size=&ak=...，"
            "再用 parse_baidu_poi() 逐条归一化。"
        )


if __name__ == "__main__":
    # 只验证纯数学部分（不需要 AK、不联网）
    print("=" * 68)
    print("百度坐标互转自测（BD-09 ↔ WGS-84）")
    print("=" * 68)
    bd = (104.072000, 30.663000)
    w = bd09_to_wgs84(*bd)
    back = wgs84_to_bd09(*w)
    print(f"BD-09 输入 : {bd}")
    print(f"WGS-84     : ({w[0]:.6f}, {w[1]:.6f})")
    print(f"回算 BD-09 : ({back[0]:.6f}, {back[1]:.6f})")
    print(f"往返误差   : {abs(back[0]-bd[0]):.2e}, {abs(back[1]-bd[1]):.2e} 度")
    print(f"当前 AK    : {'已配置' if load_baidu_ak() else '未配置（占位，不影响上面的数学验证）'}")

"""坐标系互转：WGS-84（GPS 国际标准） / GCJ-02（火星坐标，高德、腾讯） / BD-09（百度）。

为什么必须转（答辩必问，一句话版）
--------------------------------
同一个火锅店，在三种坐标系里是三个不同的经纬度，**最大能差 500 米**。
高德返回 GCJ-02，百度返回 BD-09，而人口/灯光栅格和 OSM 路网都是 WGS-84。
不做统一，把 POI 点直接落到 WGS-84 网格上，就会整体偏移几百米——
密度统计看起来"有数"，其实全错在隔壁网格。这种错**不会报错**，只会悄悄错。

调用约定
--------
- 项目内部**一律以 WGS-84 为准**（数据库存的 lng/lat 都是 WGS-84）。
- 从高德进来 → `gcj02_to_wgs84()`；从百度进来 → `bd09_to_wgs84()`。
- 反过来（例如要把网格中心发给高德做逆地理编码）→ `wgs84_to_gcj02()`。

精度说明（老实讲）
----------------
GCJ-02 是**加密偏移算法**，官方从未公开，业界用的是公开逆向拟合公式，
转换误差约 1–2 米，对"1 公里网格的密度统计"完全够用；对"门牌级导航"不够。
"""

from __future__ import annotations

import math

# ---------------------------------------------------------------- 常量（都是公开公式里的固定量）
_A = 6378245.0                 # 克拉索夫斯基椭球长半轴（米）
_EE = 0.00669342162296594323   # 第一偏心率平方
_X_PI = math.pi * 3000.0 / 180.0
_PI = math.pi

# 中国大致范围（矩形）。超出这个范围说明输入不是中国坐标，或已经是 WGS-84，
# 此时 GCJ-02 加偏公式不适用，直接原样返回。
CHINA_BBOX = (73.0, 3.0, 136.0, 54.0)


def out_of_china(lng: float, lat: float) -> bool:
    """是否在中国范围外（含粗判，用于决定要不要加偏）。"""
    min_lng, min_lat, max_lng, max_lat = CHINA_BBOX
    return not (min_lng < lng < max_lng and min_lat < lat < max_lat)


def _transform_lat(x: float, y: float) -> float:
    """公式里的纬度扰动项（照抄公开公式，不要试图"优化"）。"""
    ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * math.sqrt(abs(x))
    ret += (20.0 * math.sin(6.0 * x * _PI) + 20.0 * math.sin(2.0 * x * _PI)) * 2.0 / 3.0
    ret += (20.0 * math.sin(y * _PI) + 40.0 * math.sin(y / 3.0 * _PI)) * 2.0 / 3.0
    ret += (160.0 * math.sin(y / 12.0 * _PI) + 320 * math.sin(y * _PI / 30.0)) * 2.0 / 3.0
    return ret


def _transform_lng(x: float, y: float) -> float:
    """公式里的经度扰动项。"""
    ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * math.sqrt(abs(x))
    ret += (20.0 * math.sin(6.0 * x * _PI) + 20.0 * math.sin(2.0 * x * _PI)) * 2.0 / 3.0
    ret += (20.0 * math.sin(x * _PI) + 40.0 * math.sin(x / 3.0 * _PI)) * 2.0 / 3.0
    ret += (150.0 * math.sin(x / 12.0 * _PI) + 300.0 * math.sin(x / 30.0 * _PI)) * 2.0 / 3.0
    return ret


def _delta(lng: float, lat: float) -> tuple[float, float]:
    """算出 (d_lng, d_lat)：WGS-84 在该点与 GCJ-02 的偏移量。"""
    d_lat = _transform_lat(lng - 105.0, lat - 35.0)
    d_lng = _transform_lng(lng - 105.0, lat - 35.0)
    rad_lat = lat / 180.0 * _PI
    magic = math.sin(rad_lat)
    magic = 1 - _EE * magic * magic
    sqrt_magic = math.sqrt(magic)
    d_lat = (d_lat * 180.0) / ((_A * (1 - _EE)) / (magic * sqrt_magic) * _PI)
    d_lng = (d_lng * 180.0) / (_A / sqrt_magic * math.cos(rad_lat) * _PI)
    return d_lng, d_lat


# ---------------------------------------------------------------- GCJ-02 <-> WGS-84
def wgs84_to_gcj02(lng: float, lat: float) -> tuple[float, float]:
    """WGS-84 → GCJ-02（往"加密"方向走，直接用公式加偏）。"""
    if out_of_china(lng, lat):
        return lng, lat
    d_lng, d_lat = _delta(lng, lat)
    return lng + d_lng, lat + d_lat


def gcj02_to_wgs84(lng: float, lat: float, iterate: int = 1) -> tuple[float, float]:
    """GCJ-02 → WGS-84（反解：先按正向公式减掉偏移，再用 round-trip 迭代修正）。

    参数 iterate 是**迭代次数**：
      - 1 次（默认）：误差约 1–2 米，够 1 km 网格用；
      - 2–3 次：误差降到 0.1 米以内，代价是多算几次三角函数。

    ★★★ 老大必改一处（二选一，改完这轮才算结束）★★★
    选项 A：把默认值 `iterate: int = 1` 改成 3，跑 `.\python.exe datasource\coord_transform.py`
            观察自测里 `round-trip 误差` 那一行从小数点后 5 位量级降到 7 位量级。
    选项 B：在下面 `for _ in range(iterate)` 里把 `if abs(d_lng) < 1e-9 and abs(d_lat) < 1e-9: break`
            提前终止条件改成 `1e-12`，让迭代更"贪心"，同样看误差那一行的变化。
    两种改法都只影响精度不影响接口，改完把自测输出贴回来。
    """
    if out_of_china(lng, lat):
        return lng, lat
    wgs_lng, wgs_lat = lng, lat
    for _ in range(max(1, iterate)):
        # 正向算出"如果这是 WGS 点，加偏后应该在哪"
        g_lng, g_lat = wgs84_to_gcj02(wgs_lng, wgs_lat)
        # 用差值把候选点往回拉
        d_lng, d_lat = g_lng - lng, g_lat - lat
        wgs_lng -= d_lng
        wgs_lat -= d_lat
        if abs(d_lng) < 1e-9 and abs(d_lat) < 1e-9:
            break
    return wgs_lng, wgs_lat


# ---------------------------------------------------------------- BD-09 <-> WGS-84
def bd09_to_gcj02(bd_lng: float, bd_lat: float) -> tuple[float, float]:
    """BD-09 → GCJ-02（百度的偏移是在 GCJ-02 基础上再叠一层）。"""
    x, y = bd_lng - 0.0065, bd_lat - 0.006
    z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * _X_PI)
    theta = math.atan2(y, x) - 0.000003 * math.cos(x * _X_PI)
    return z * math.cos(theta), z * math.sin(theta)


def gcj02_to_bd09(gcj_lng: float, gcj_lat: float) -> tuple[float, float]:
    """GCJ-02 → BD-09。"""
    z = math.sqrt(gcj_lng * gcj_lng + gcj_lat * gcj_lat) + 0.00002 * math.sin(gcj_lat * _X_PI)
    theta = math.atan2(gcj_lat, gcj_lng) + 0.000003 * math.cos(gcj_lng * _X_PI)
    return z * math.cos(theta) + 0.0065, z * math.sin(theta) + 0.006


def bd09_to_wgs84(bd_lng: float, bd_lat: float, iterate: int = 3) -> tuple[float, float]:
    """BD-09 → WGS-84：先退到 GCJ-02，再反解到 WGS-84。"""
    gcj_lng, gcj_lat = bd09_to_gcj02(bd_lng, bd_lat)
    return gcj02_to_wgs84(gcj_lng, gcj_lat, iterate=iterate)


def wgs84_to_bd09(lng: float, lat: float) -> tuple[float, float]:
    """WGS-84 → BD-09。"""
    gcj_lng, gcj_lat = wgs84_to_gcj02(lng, lat)
    return gcj02_to_bd09(gcj_lng, gcj_lat)


# ---------------------------------------------------------------- 两个实用小工具
def distance_m(lng1: float, lat1: float, lng2: float, lat2: float) -> float:
    """两点球面距离（米），Haversine。用来量化"转不转坐标系差多少"。"""
    r = 6371008.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lng2 - lng1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(h))


def roundtrip_error(lng: float, lat: float) -> tuple[float, float]:
    """自测用：GCJ→WGS→GCJ 来回一圈，返回 (经度误差, 纬度误差)，单位度。"""
    w_lng, w_lat = gcj02_to_wgs84(lng, lat)
    b_lng, b_lat = wgs84_to_gcj02(w_lng, w_lat)
    return abs(b_lng - lng), abs(b_lat - lat)


if __name__ == "__main__":
    # 天安门（已知 GCJ-02 约 116.397428, 39.90923）
    cases = [
        ("天安门", 116.397428, 39.90923),
        ("成都天府广场", 104.065735, 30.659462),
        ("武侯祠", 104.043497, 30.647043),
    ]
    print("=" * 68)
    print("坐标系互转自测（输入按 GCJ-02 处理，高德就是这个坐标系）")
    print("=" * 68)
    for name, g_lng, g_lat in cases:
        w_lng, w_lat = gcj02_to_wgs84(g_lng, g_lat)
        shift = distance_m(g_lng, g_lat, w_lng, w_lat)
        e_lng, e_lat = roundtrip_error(g_lng, g_lat)
        print(f"{name:<10} GCJ=({g_lng:.6f},{g_lat:.6f})")
        print(f"{'':<10} WGS=({w_lng:.6f},{w_lat:.6f})  偏移 {shift:6.1f} 米  "
              f"round-trip 误差=({e_lng:.2e},{e_lat:.2e}) 度")
    print("-" * 68)
    print("看点：偏移量在 100–600 米之间 → 不转坐标系，密度统计就会整体错一格。")

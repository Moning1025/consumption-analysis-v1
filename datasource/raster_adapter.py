"""栅格采样适配器：从 GeoTIFF 里按"网格"取值（人口 / 夜间灯光）。

为什么不直接 `import rasterio` 就叫它读
--------------------------------------
两个 TIF 一个 534 MB、一个 878 MB。整读进内存 = 一次吃 1.4 GB，
普通笔记本（8–16 GB）会直接卡死甚至被系统杀掉。
所以这里的核心设计是**只读 bbox 那一小块**（windowed read）：
成都主城区那点范围，灯光 TIF 通常只占几千个像元，几十 KB 内存。

三个技术点（答辩可以讲）
------------------------
1. **窗口读取**：`rasterio.windows.from_bounds()` 把经纬度 bbox 换算成像素行列窗口，
   `ds.read(1, window=...)` 只读这块，不碰其余 800 MB。
2. **坐标系对齐**：栅格可能是地理坐标（EPSG:4326，度）也可能是投影坐标（米）。
   用 `ds.crs` 判断；不是 4326 就用 pyproj 把网格中心从经纬度投到栅格坐标系，
   否则 `ds.index()` 会算出天文数字的行列号。
3. **Nodata / NaN 处理**：海洋、边界外是 nodata（常见 -9999 或 0），
   必须 mask 掉再求均值，否则"灯光均值"会被 -9999 拉成负数。

★ 老大必改一处（二选一，改完这轮才算结束）
------------------------------------------
打开 `RasterSampler.sample_cells()`，找到 `mode` 分支：
  选项 A（验"3×3 邻域"）：把 `"block"` 分支里的整格切片改成**只取该格中心像元周围 3×3**，
      即 `rows_slice / cols_slice` 各向外扩 ±1 后取，再看看 verify_s2_1d.py 输出里
      `light_intensity` 的 TOP5 数值变化（会更平滑、极端值被压掉）。
  选项 B（验"中位数更抗噪"）：把 `np.nanmean` 改成 `np.nanmedian`，
      同样看 TOP5 变化——中位数对单个亮斑（如机场跑道灯）不敏感。
改完把 `verify_s2_1d.py` 的 TOP5 那几行贴回来，并说明为什么原方案选了 mean。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

# 单次窗口读取的像元上限：超过就说明"这个 bbox 不该一次性读"，直接报错而不是硬扛到 OOM
MAX_WINDOW_PIXELS = 80_000_000


@dataclass(slots=True)
class GridCell:
    """一个网格（经纬度 bbox + 中心点）。lng/lat 均为 WGS-84。"""

    grid_id: str
    lng: float          # 中心经度
    lat: float          # 中心纬度
    lng_min: float
    lat_min: float
    lng_max: float
    lat_max: float


class RasterSampler:
    """按网格从 GeoTIFF 取值。构造即打开数据集（rasterio 是懒加载，不会读全部数据）。"""

    def __init__(self, path: str | Path, band: int = 1) -> None:
        try:
            import rasterio  # 局部导入：没装也能 import 本模块（只在真正采样时报错）
        except ImportError as exc:  # pragma: no cover - 环境相关
            raise ImportError(
                "缺少 rasterio：请先执行 "
                "D:\\Python\\venv_p3\\Scripts\\python.exe -m pip install --only-binary :all: rasterio"
            ) from exc

        self._rasterio = rasterio
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"栅格文件不存在：{self.path}")
        self.band = band
        self._ds = rasterio.open(self.path)
        self.crs = self._ds.crs
        self.nodata = self._ds.nodata
        self.width = self._ds.width
        self.height = self._ds.height
        self.res = self._ds.res  # (xres, yres)，地理坐标下单位是"度"

    # ------------------------------------------------------------------ 资源管理
    def close(self) -> None:
        self._ds.close()

    def __enter__(self) -> "RasterSampler":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ------------------------------------------------------------------ 内部工具
    def _to_raster_xy(self, lngs, lats):
        """把一批 WGS-84 经纬度转到栅格自己的坐标系。

        栅格是 EPSG:4326 时原样返回（省一次 pyproj 调用）；
        否则（例如 Albers 投影）用 pyproj Transformer 批量转，快且准。
        """
        epsg = self.crs.to_epsg() if self.crs else 4326
        if epsg == 4326 or epsg is None:
            return lngs, lats
        from pyproj import Transformer  # 局部导入

        transformer = Transformer.from_crs("EPSG:4326", self.crs, always_xy=True)
        xs, ys = transformer.transform(lngs, lats)
        return xs, ys

    def _bbox_rowcol(self, cell: GridCell) -> tuple[int, int, int, int] | None:
        """把网格 bbox 换算成 (row_start, row_stop, col_start, col_stop)；完全落在栅格外返回 None。"""
        xs, ys = self._to_raster_xy(
            [cell.lng_min, cell.lng_max, cell.lng_min, cell.lng_max],
            [cell.lat_min, cell.lat_min, cell.lat_max, cell.lat_max],
        )
        r0, c0 = self._ds.index(min(xs), max(ys))            # 左上角 → (行, 列)
        r1, c1 = self._ds.index(max(xs), min(ys))            # 右下角
        r_start, r_stop = min(r0, r1), max(r0, r1) + 1
        c_start, c_stop = min(c0, c1), max(c0, c1) + 1
        r_start = max(0, r_start)
        c_start = max(0, c_start)
        r_stop = min(self.height, r_stop)
        c_stop = min(self.width, c_stop)
        if r_stop <= r_start or c_stop <= c_start:
            return None
        return r_start, r_stop, c_start, c_stop

    # ------------------------------------------------------------------ 核心：按网格采样
    def sample_cells(
        self,
        cells: list[GridCell],
        mode: str = "mean",
        block: int = 2048,
    ) -> dict[str, float | None]:
        """对一批网格取样，返回 {grid_id: 值}。

        mode:
          - "mean"：格内像元**均值**（灯光强度用这个，单位 nW/cm2/sr）
          - "sum" ：格内像元**求和**（人口用这个：每个像元本身就是"这个像元里有多少人"）

        实现：分块（block×block 像元）读，避免一次吃满内存；
        每块读完就地算，读完即丢，内存峰值约 block²×8 字节 ≈ 32 MB。
        """
        import numpy as np

        results: dict[str, float | None] = {}

        # 先把所有网格的行列窗口算出来，并按行号排序——顺序读磁盘比随机读快一个数量级
        prepared: list[tuple[GridCell, tuple[int, int, int, int]]] = []
        for cell in cells:
            window = self._bbox_rowcol(cell)
            if window is None:
                results[cell.grid_id] = None      # 落在栅格外：明确记 None，不要糊一个 0
                continue
            prepared.append((cell, window))
        prepared.sort(key=lambda item: item[1][0])

        for cell, (r_start, r_stop, c_start, c_stop) in prepared:
            if (r_stop - r_start) * (c_stop - c_start) > MAX_WINDOW_PIXELS:
                raise MemoryError(
                    f"网格 {cell.grid_id} 命中 {r_stop - r_start}×{c_stop - c_start} 个像元，"
                    "超过安全上限；请缩小 bbox 或调大网格边长。"
                )
            from rasterio.windows import Window  # 局部导入，避免顶层强依赖

            window = Window.from_slices((r_start, r_stop), (c_start, c_stop))
            data = self._ds.read(self.band, window=window, masked=True)
            arr = data.astype("float64").filled(np.nan)
            if self.nodata is not None:
                arr[arr == self.nodata] = np.nan
            # 有些产品用 -9999 / -3.4e38 表示 nodata 但声明为空：兜一层常识判断
            arr[arr < -1e30] = np.nan

            if arr.size == 0 or np.all(np.isnan(arr)):
                results[cell.grid_id] = None
                continue

            if mode == "sum":
                results[cell.grid_id] = float(np.nansum(arr))
            elif mode == "mean":
                results[cell.grid_id] = float(np.nanmean(arr))
            else:
                raise ValueError(f"未知 mode：{mode}（只支持 mean / sum）")

        return results

    # ------------------------------------------------------------------ 诊断信息
    def describe(self) -> str:
        epsg = self.crs.to_epsg() if self.crs else "未知"
        return (
            f"{self.path.name}: {self.width}×{self.height} 像元, "
            f"CRS={self.crs}, EPSG={epsg}, 分辨率={self.res}, nodata={self.nodata}"
        )

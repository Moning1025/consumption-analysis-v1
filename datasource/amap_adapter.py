"""高德地图 Web 服务 · POI 搜索适配器（S2-1a，第二层代码）。

分层归属（工作区规则 v2）
--------------------------------------------------------------------
- **第二层**（沫宁鱼写完整版 + 逐行注释 + 指定一处必须你改）：
  本文件。你要改的地方只有一处，见文件末尾 `_sleep_before_retry`。
- 第一层（不用你改、只跑）：`scripts/fetch_amap_poi.py`、`scripts/verify_s2_1a.py`。
- 第三层（你自己写）：本轮为空。等清洗阶段才给你留核心逻辑。

它到底解决什么问题
--------------------------------------------------------------------
把“按关键词搜某城市的 POI”封装成一个可复用对象，专门兜住 4 件最容易炸的事：

1. **密钥**：从环境变量读，绝不写进源码（源码要进 GitHub）。
2. **节流**：高德个人开发者 QPS 很低（基本 3 次/秒级别），请求挨太紧会返回
   `infocode=10004`（单位时间内访问过于频繁）。所以每次请求之间强制留间隔。
3. **重试**：网络抖动、超时、偶发 10004，自动重试；但**配额类错误不重试**
   （配额已经用完了，重试只会更糟，还白白浪费一次额度）。
4. **错误码翻译**：高德 `status="0"` 时不抛原始 JSON，而是抛带人话的异常，
   你在命令行一眼能看懂“是 Key 错了还是今天额度用完了”。

先看这两个文档再读代码（参数名必须对得上）
--------------------------------------------------------------------
- 接口说明：https://lbs.amap.com/api/webservice/guide/api/search
  关键请求参数：`key` / `keywords` / `types` / `city` / `citylimit` /
  `offset`（每页条数，最大 25）/ `page`（第几页）/ `extensions`（all 才返回 tel 等）
- 错误码表：https://lbs.amap.com/api/webservice/guide/tools/info

用法（在别的脚本里）
--------------------------------------------------------------------
    from datasource.amap_adapter import AmapClient

    client = AmapClient()                      # key 自动从环境变量取
    for poi in client.iter_keyword("餐饮", city="成都", pages=20):
        print(poi["name"], poi["adcode"])
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Iterator

import requests

logger = logging.getLogger(__name__)

# 高德 POI 搜索接口（v3 版，字段稳定、文档齐全）
AMAP_SEARCH_URL = "https://restapi.amap.com/v3/place/text"

# 密钥可能被老大 setx 成不同名字，这里按顺序找第一个非空的。
# 你只要命中最前面那个名字就行；一个都没有时，构造函数会直接报错并告诉你怎么配。
KEY_ENV_CANDIDATES = (
    "AMAP_WEB_KEY",
    "AMAP_KEY",
    "AMAP_WEBSERVICE_KEY",
    "GAODE_KEY",
)

# 高德的错误码 → 人话。只列实际会撞到的，剩下的一律带上原始 infocode。
ERROR_MESSAGE = {
    "10001": "Key 不正确或已过期（检查环境变量里的 Key，注意别把 Web服务 的 Key 用成了 Web端JS API 的）",
    "10003": "今日访问量已超配额（个人开发者每日额度很小，明天再跑，或去控制台看配额）",
    "10004": "单位时间内访问过于频繁（调大 min_interval，比如 0.5~1.0 秒）",
    "10005": "IP 白名单校验失败（控制台里给这个 Key 配了 IP 白名单，本机 IP 不在里面）",
    "10009": "请求 Key 与绑定平台不匹配（Key 的“服务平台”必须选 Web服务）",
    "10044": "账号维度日调用量超限（整个账号的额度用完了，不是单个 Key 的问题）",
    "20000": "请求参数非法（多半是 offset 超过 25，或 page 超过 100）",
    "20001": "缺少必填参数（keywords / types / city 至少给一个）",
    "20003": "未知错误（高德偶发，重试一次通常就好）",
    "20800": "规划点（起点/终点/途经点）不在支持范围内",
    "20802": "规划点（起点/终点/途经点）不在中国范围内",
}

# 这些错误码重试有意义（偶发/限流），其余重试纯属浪费额度
RETRYABLE_INFOCODES = {"10004", "20003"}


class AmapError(RuntimeError):
    """高德业务错误（HTTP 通了，但 status="0"）。

    带三个字段，方便上层决定“跳过还是终止”：
    - infocode：高德错误码，例如 "10004"
    - info：高德原始说明
    - retryable：这次错误重试有没有意义
    """

    def __init__(self, infocode: str, info: str, retryable: bool = False):
        self.infocode = str(infocode)
        self.info = info
        self.retryable = retryable
        human = ERROR_MESSAGE.get(self.infocode, f"未收录的高德错误码，原始说明：{info}")
        super().__init__(f"[高德 {self.infocode}] {human}")


def resolve_api_key() -> str:
    """按候选顺序找出第一个非空的环境变量值；找不到就报错并给出配置命令。"""
    for name in KEY_ENV_CANDIDATES:
        value = (os.getenv(name) or "").strip()
        if value:
            logger.debug("使用环境变量 %s 作为高德 Key", name)
            return value
    joined = " / ".join(KEY_ENV_CANDIDATES)
    raise AmapError(
        "ENV",
        f"环境变量里没有找到高德 Key（找过：{joined}）。"
        f"在 PowerShell 里执行：setx {KEY_ENV_CANDIDATES[0]} \"你的Key\""
        f" —— 然后**关掉并重开终端/PyCharm**，setx 只对新进程生效。",
    )


class AmapClient:
    """高德 POI 搜索客户端。

    参数
    ----
    api_key       : 不给就从环境变量读（推荐不传）
    timeout       : 单次 HTTP 超时（秒）。高德通常 <300ms，5 秒足够
    min_interval  : 两次请求之间**至少**间隔多少秒（节流）。默认 0.35 ≈ 每秒 3 次以内
    max_retries   : 单次请求最多重试几次（不含首次）
    session       : 可注入自己建的 Session（测试用）
    """

    def __init__(
        self,
        api_key: str | None = None,
        timeout: float = 5.0,
        min_interval: float = 0.35,
        max_retries: int = 3,
        session: requests.Session | None = None,
    ):
        self.api_key = api_key or resolve_api_key()
        self.timeout = timeout
        self.min_interval = min_interval
        self.max_retries = max_retries
        self.session = session or requests.Session()
        # 记下“上一次请求结束的时刻”，用来算还需要等多久
        self._last_call_finished_at = 0.0
        # 统计信息：一次运行结束打印出来，方便写进采集账本
        self.request_count = 0
        self.retry_count = 0
        self._retry_base = 1.0  # 指数退避的基础延迟时间（秒）

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _throttle(self) -> None:
        """节流：保证相邻两次请求之间至少隔 min_interval 秒。

        为什么是“距上次**结束**算起”而不是“距上次开始算起”：
        如果按开始算，一次慢请求（比如 2 秒才返回）之后会立刻发下一条，
        等于完全没有间隔，高德那边看到的就是突发流量。
        """
        if self._last_call_finished_at <= 0:
            return
        elapsed = time.monotonic() - self._last_call_finished_at
        remaining = self.min_interval - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def _sleep_before_retry(self, attempt: int) -> None:
        """第 attempt 次重试（从 1 开始）之前等待多久。

        ⚠️⚠️ 老大必改一处（二选一，改完把改后的代码贴给我看）⚠️⚠️

        现状：不管第几次重试，都固定睡 1.0 秒。

        选项 A（推荐，答辩更站得住）：改成**指数退避**
            第 1 次重试睡 1 秒，第 2 次 2 秒，第 3 次 4 秒。
            一行提示：`delay = self._retry_base * (2 ** (attempt - 1))`
            要改的地方：把下面 `delay = 1.0` 换掉，并在 __init__ 里加
            `self._retry_base = 1.0`（或者直接用常量 1.0 乘）。

        选项 B：保留固定间隔，但加一点**随机抖动**
            `delay = 1.0 + random.uniform(0, 0.5)`
            好处：多个脚本同时跑时不会“整齐地一起撞上去”。

        改完怎么验证（两种都适用）：
        把 min_interval 临时改成 0 也不会轻易触发；真正的验证办法是
        在 `_request` 里打印每次重试的等待秒数，断网跑一次，看到
        1.0 → 2.0 → 4.0（选项 A）或 1.0~1.5 随机（选项 B）即为改对。
        """
        # 指数退避 (1s, 2s, 4s, 8s...)
        delay = self._retry_base * (2 ** (attempt - 1))
        # 强烈建议加个上限（例如最多等 30 秒），防止重试次数多时等待时间过长（指数爆炸）
        delay = min(delay, 30.0)
        logger.warning("第 %s 次重试, 先等 %.1f 秒", attempt, delay)
        time.sleep(delay)

    def _request(self, params: dict) -> dict:
        """发一次请求，返回高德 JSON。网络抖动自动重试，配额错误立刻抛。"""
        query = dict(params)
        query["key"] = self.api_key

        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            if attempt > 0:                       # 第一次不算重试
                self.retry_count += 1
                self._sleep_before_retry(attempt)

            self._throttle()                      # 每次真发请求前都节流
            self.request_count += 1
            try:
                response = self.session.get(
                    AMAP_SEARCH_URL, params=query, timeout=self.timeout
                )
            except requests.RequestException as exc:      # 断网 / 超时 / DNS 挂了
                last_error = exc
                logger.warning("请求异常（第 %s 次）：%s", attempt + 1, exc)
                continue                                  # 交给下一轮重试
            finally:
                # 不管成功失败都记时刻，否则失败后会立刻又发一条
                self._last_call_finished_at = time.monotonic()

            if response.status_code != 200:
                last_error = RuntimeError(f"HTTP {response.status_code}")
                logger.warning("HTTP %s（第 %s 次）", response.status_code, attempt + 1)
                continue

            payload = response.json()

            if str(payload.get("status")) == "1":
                return payload

            # status=0：业务错误。限流类可以重试，其余立刻失败。
            error = AmapError(
                payload.get("infocode", "unknown"),
                payload.get("info", ""),
                retryable=str(payload.get("infocode")) in RETRYABLE_INFOCODES,
            )
            if error.retryable and attempt < self.max_retries:
                last_error = error
                logger.warning("可重试错误：%s", error)
                continue
            raise error

        # 重试次数用完了
        raise RuntimeError(f"高德请求连续失败 {self.max_retries + 1} 次：{last_error}")

    # ------------------------------------------------------------------
    # 对外接口
    # ------------------------------------------------------------------
    def search_text(
        self,
        keywords: str | None = None,
        city: str | None = None,
        types: str | None = None,
        page: int = 1,
        offset: int = 25,
        extensions: str = "all",
        citylimit: bool = True,
    ) -> list[dict]:
        """按关键词/类型搜一页 POI，返回**解析后**的字典列表（不是原始 JSON）。

        `types` 与 `keywords` 至少给一个，详见官方文档的“POI 分类码”：
        https://lbs.amap.com/api/webservice/download
        """
        params = {
            "keywords": keywords or "",
            "city": city or "",
            "types": types or "",
            "citylimit": "true" if citylimit else "false",
            "offset": max(1, min(int(offset), 25)),   # 高德硬限制：每页最多 25 条
            "page": max(1, min(int(page), 100)),      # 高德硬限制：最多翻到 100 页
            "extensions": extensions,
            "output": "json",
        }
        payload = self._request(params)
        raw_items = payload.get("pois") or []
        return [self.parse_poi(item, keyword=keywords or types or "") for item in raw_items]

    def iter_keyword(
        self,
        keywords: str,
        city: str | None = None,
        types: str | None = None,
        pages: int = 1,
        offset: int = 25,
    ) -> Iterator[dict]:
        """翻页生成器：从第 1 页翻到第 pages 页，逐条吐出解析后的 POI。

        用生成器（yield）而不是一次性返回大列表，是因为采集几千条时
        没必要把全部数据先堆在内存里再入库。
        """
        for page in range(1, pages + 1):
            items = self.search_text(
                keywords=keywords, city=city, types=types, page=page, offset=offset
            )
            logger.info("关键词=%s 第 %s 页，返回 %s 条", keywords, page, len(items))
            if not items:
                break          # 翻到空页说明没数据了，提前收工
            for item in items:
                yield item

    @staticmethod
    def parse_poi(item: dict, keyword: str = "") -> dict:
        """把高德一条 POI 原始字典，转成 poi_merchant 表的字段字典。

        高德把经纬度塞在 `location` 字符串里（"104.06,30.67"），
        而我们要算距离、做空间分箱，必须拆成两个 REAL 列。
        """
        raw_location = (item.get("location") or "").strip()
        longitude = latitude = None
        if "," in raw_location:
            lon_text, _, lat_text = raw_location.partition(",")
            try:
                longitude = float(lon_text)
                latitude = float(lat_text)
            except ValueError:
                logger.warning("经纬度解析失败：%r", raw_location)

        # 高德的 type 形如 "餐饮服务;中餐厅;火锅店"，取前两段做业态层级
        type_parts = [p.strip() for p in (item.get("type") or "").split(";") if p.strip()]

        return {
            "poi_id": item.get("id") or "",
            "name": item.get("name") or "",
            "category_l1": type_parts[0] if type_parts else None,
            "category_l2": type_parts[1] if len(type_parts) > 1 else None,
            "amap_type": item.get("type") or "",
            "typecode": item.get("typecode") or "",
            "address": item.get("address") if isinstance(item.get("address"), str) else "",
            "province": item.get("pname") or "",
            "city": item.get("cityname") or "",
            "district": item.get("adname") or "",
            "adcode": item.get("adcode") or "",
            "longitude": longitude,
            "latitude": latitude,
            "location_text": raw_location,
            "tel": item.get("tel") if isinstance(item.get("tel"), str) else "",
            "keyword": keyword,
            "raw_json": json.dumps(item, ensure_ascii=False),
        }

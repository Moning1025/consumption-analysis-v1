"""Pandas vs 纯 Python 循环：分组聚合性能对比实验。

【分层标注 · 按工作区规则 v2】
- build_test_rows()            → 第一层（数据生成）：沫宁鱼写完整版，老大只跑
- _timeit / _check_same / main → 第一层（样板：计时、校验、打印）：老大只跑
- group_sum_with_loop()        → 第二层：完整版 + 逐行注释，老大必须改一处
- group_sum_with_pandas()      → 第二层：完整版 + 逐行注释，老大必须改一处
- 第三层本轮为空：Pandas 是第一个知识点，先读懂再动手；下一轮清洗模块才留核心逻辑

老大本轮要做的是两处「必改」，都不改结果正确性、只改写法（每处给两个选项，二选一）：
  1. group_sum_with_loop 里：CATEGORIES 加第 6 个品类 / 累加器换成 collections.defaultdict(float)
  2. group_sum_with_pandas 里：.sum() 换成 .agg({"consumption_amount": "sum"}) / 换成 .agg(["sum","count"])
两处都改完、脚本仍跑通、并能解释差异，本轮才算结束。

运行方式（venv 固定在 D:\Python\venv_p3，项目里没有 venv）：
    D:\Python\venv_p3\Scripts\python.exe scripts\exp_pandas_vs_loop.py

答辩用途：为预判卡 B1 拿到「10 万行分组求和快 X 倍」里的 X。
"""

from __future__ import annotations

import random
import statistics
import time
from collections.abc import Callable
from datetime import date, timedelta


# ============================================================
# 常量：测试数据的取值域（照抄项目真实业务，答辩可讲“基于真实字段结构”）
# ============================================================

ROWS = 100_000      # 数据规模
REPEAT = 3          # 每个版本跑几次取中位数

MERCHANTS = [
    "蜀香园火锅(春熙路店)", "锦官城川菜馆", "熊猫主题文创店", "宽窄巷子茶馆",
    "川西坝子火锅(高新店)", "蓉城连锁超市", "天府旅游纪念品店", "老码头串串香",
]
CATEGORIES = ["餐饮", "零售", "文旅", "交通", "娱乐", "其他"]
REGIONS = ["成都市", "绵阳市", "德阳市", "宜宾市", "南充市", "泸州市"]
START_DATE = date(2026, 1, 1)
SEED = 20261104     # 固定种子：每次跑出来的数据一模一样，结果可复现


# ============================================================
# 第一层：造 10 万行测试数据（沫宁鱼写的，老大只跑）
# ============================================================

def build_test_rows(n_rows: int = ROWS) -> list[dict]:
    """造 n_rows 行数据，字段与 consumption_record 表一致。

    返回 list[dict]，每个 dict 恰好这 5 个键：
        merchant_name       str    门店名（8 个四川真实风格门店里随机挑）
        consumption_amount  float  8.0 ~ 1200.0 元，保留 2 位小数
        category_label      str    餐饮/零售/文旅/交通/其他
        city_region         str    川内 6 个城市
        occurred_date       str    "YYYY-MM-DD"，2026 年内随机一天

    为什么这么写（读一遍就懂，答辩可能被问“数据怎么造的”）：
      1. 每个字段的值域都是从项目真实业务里抄的，不是 random 乱造：
         merchant_name       8 个成都/四川风格门店名（春熙路、宽窄巷子、天府这些真地名），
                             对应用户表里能出现的商户，且刻意让“连锁店”带括号分店后缀；
         consumption_amount  8.0 ~ 1200.0 元 —— 餐饮客单价到文旅高客单的区间，
                             再 round(…, 2) 保留两位小数，跟库里 DECIMAL 语义一致；
         category_label      5 类（餐饮/零售/文旅/交通/其他）—— 与消费分类口径对齐，
                             5 类也正好是 RFM 默认展示 5 层的粒度；
         city_region         川内 6 个市（成都/绵阳/德阳/宜宾/南充/泸州）—— 演示区域是四川，
                             含省会 + 5 个地级市，够做地域归一化和热力分布；
         occurred_date       2026 全年 365 天随机一天，输出 "YYYY-MM-DD" 文本，
                             因为库里 occurred_date 存的就是文本日期，不做类型转换。
      2. 用 random.Random(SEED) 造独立的随机源，而不是 random.seed() 改全局状态。
         —— 前者只影响本函数，后者会把整个程序的随机序列都钉死，容易坑到别处。
      3. 固定种子 = 可复现：同样的机器、同样的代码，每次跑出来的 10 万行完全一致。
         —— 答辩被问“你换个时间跑数字会不会变”，答案就是“数据不变，只有耗时变”。
      4. 种子只在循环**外**创建一次（rng = random.Random(SEED) 那一行），循环里只向它要随机数。
         —— 如果每次迭代都重建随机源，10 万行会变成 10 万个一模一样的“第一行”，
            分组也就只剩 1 类，实验直接作废；同时这个随机源对象本身只建一次，
            不会在 10 万次循环里反复申请内存，把造数据的时间也算进实验里。
      5. 一次性用 dict 字面量建好整行，而不是先建空 dict 再逐个赋值。
         —— 后者在 10 万次循环里会多出 50 万次属性写入，白白把造数据的时间也算进去。
      6. 日期用 date + timedelta 算再 isoformat()，因为项目里 occurred_date 存的就是
         文本日期（"2026-05-01"），不做类型转换，两边输入才完全一样。
    """
    rng = random.Random(SEED)                    # 独立随机源，种子固定
    rows: list[dict] = []
    for _ in range(n_rows):
        rows.append({
            "merchant_name": rng.choice(MERCHANTS),
            "consumption_amount": round(rng.uniform(8.0, 1200.0), 2),
            "category_label": rng.choice(CATEGORIES),
            "city_region": rng.choice(REGIONS),
            "occurred_date": (START_DATE + timedelta(days=rng.randrange(365))).isoformat(),
        })
    return rows


# ============================================================
# 第二层：循环版（沫宁鱼给完整版 + 逐行注释；老大必须改一处 → 见文末「必改一处」）
# ============================================================

def group_sum_with_loop(rows: list[dict]) -> dict[str, float]:
    """按 category_label 分组，累加 consumption_amount，返回 {分类: 金额合计}。

    输入：rows = build_test_rows() 造出来的 list[dict]，每行 5 个字段
    输出：dict[str, float]，键是 5 个分类之一，值是该分类金额总和
    关键约束：
      - 必须**一趟循环**累加完成（时间复杂度 O(行数)）
      - 不允许“按分类过滤再 sum”的写法，例如
            {c: sum(r["consumption_amount"] for r in rows if r["category_label"] == c)
             for c in CATEGORIES}
        那是 O(分类数 × 行数)，会让循环版白白吃亏、X 虚高，答辩一追问就站不住
    查哪个文档（补全了参数名）：
        Python 官方教程 dict 章节
          https://docs.python.org/3/tutorial/datastructures.html#dictionaries
          重点：dict.get(key, default) —— 第二个参数是“键不存在时返回的默认值”
        dict 方法总表
          https://docs.python.org/3/library/stdtypes.html#dict.get
          重点：dict.get / dict.setdefault / dict.items
        collections.defaultdict（必改一处的选项 B 要用）
          https://docs.python.org/3/library/collections.html#collections.defaultdict
          重点：defaultdict(float) 的 default_factory 参数
    """
    acc: dict[str, float] = {}                               # ① 累加容器：分类 -> 金额合计；空 dict 起步
    for row in rows:                                          # ② 只遍历一次，10 万行就是 10 万次
        category = row["category_label"]                       # ③ 取出这一行的分类（键名必须与 build_test_rows 逐字一致）
        amount = row["consumption_amount"]                     # ④ 取出这一行的金额（float）
        acc[category] = acc.get(category, 0.0) + amount        # ⑤ 核心一行：取当前累计值（没有就当 0.0），加上本次金额，写回
    return acc                                                # ⑥ 返回 {分类: 合计}，与 Pandas 版同类型，才能对得上


# ============================================================
# 第二层：Pandas 版（同上，完整版 + 逐行注释；老大必须改一处 → 见文末「必改一处」）
# ============================================================

def group_sum_with_pandas(rows: list[dict]) -> dict[str, float]:
    """同一件事的 Pandas 实现，返回 {分类: 金额合计}。

    输入：与循环版完全相同的 list[dict]
    输出：dict[str, float]，内容与循环版一致（顺序不要求，_check_same 只比内容）
    关键约束：
      - 函数内部 import pandas as pd（不写在文件顶部，这样没装 pandas 也能单独跑循环版）
      - 只用 groupby + sum，不要用 .apply(lambda) 逐行处理（那会慢到失真，实验就废了）
    查哪个文档（补全了参数名）：
        DataFrame 构造
          https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html
          重点：DataFrame(data) 里 data 可以是 list[dict]，dict 的键自动变列名
        DataFrame.groupby（by 参数）
          https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.groupby.html
          重点：groupby(by="category_label") —— by 传列名就是按这一列分组
        GroupBy.sum
          https://pandas.pydata.org/docs/reference/api/pandas.core.groupby.DataFrameGroupBy.sum.html
        GroupBy.agg（必改一处两个选项都要用）
          https://pandas.pydata.org/docs/reference/api/pandas.core.groupby.DataFrameGroupBy.agg.html
          重点：agg("sum") / agg({"consumption_amount": "sum"}) / agg(["sum", "count"]) 三种形式
        Series.to_dict
          https://pandas.pydata.org/docs/reference/api/pandas.Series.to_dict.html
    """
    import pandas as pd                                       # ① 局部导入：pandas 没装也不影响循环版
    df = pd.DataFrame(rows)                                   # ② list[dict] -> 表格；每个键变成一列
    totals = df.groupby(by="category_label")["consumption_amount"].agg("sum")  # ③ by 指定分组列，只留金额列再求和 → Series
    return totals.to_dict()                                   # ④ Series（索引=分类）转普通 dict，和循环版同类型


# ============================================================
# 老大必改一处（每个函数二选一，改完这轮才算结束）
# ============================================================
#
# 【必改 A】group_sum_with_loop 里二选一：
#   选项 A1（改数据域，最简单）：把第 41 行
#           CATEGORIES = ["餐饮", "零售", "文旅", "交通", "其他"]
#       改成 6 个分类，例如末尾加 "娱乐"。
#       预期差异：输出最后一行「分组数 5」→「分组数 6」；「样本一行」里的分类/金额会变
#       （随机数的消耗序列变了，数据本身就不同了，这是正常的），但两版仍必须一致。
#       要点：分类维度变了，聚合结果的“键集合”跟着变，但两版依然必须一致——
#             这正是本次改动的验收点：分组数变多，_check_same 仍然通过。
#   选项 A2（改写法，不动数据）：把第 125 行 acc 的定义
#           acc: dict[str, float] = {}
#       改成  from collections import defaultdict
#              acc: dict[str, float] = defaultdict(float)   （这行 import 写到文件顶部）
#       并把第 129 行改成
#           acc[category] += amount
#       预期差异：所有输出**逐字一致**（除耗时与倍率数值），因为 defaultdict(float)
#       在“键不存在时自动给 0.0”，语义与 acc.get(category, 0.0) 完全等价。
#       要点：等价重构不改输出——这也是验收方式：输出没变，说明你真改对了。
#
# 【必改 B】group_sum_with_pandas 里二选一：
#   选项 B1（换成 agg 字典写法）：把第 162 行改成
#           totals = df.groupby(by="category_label").agg({"consumption_amount": "sum"})
#           return totals["consumption_amount"].to_dict()
#       预期差异：所有输出逐字一致（agg 的字典形式 {"列名": "函数名"} 与先选列再 .sum() 等价），
#       但返回前多了一步按列名取值——因为 agg(字典) 给你的是 DataFrame，不是 Series。
#       要点：Series 与 DataFrame 的区别，就在“直接 .to_dict() 会不会变成嵌套 dict”。
#   选项 B2（加 count 指标）：把第 162 行改成
#           stats = df.groupby(by="category_label")["consumption_amount"].agg(["sum", "count"])
#           print("每类笔数：", stats["count"].to_dict())
#           totals = stats["sum"]
#       预期差异：输出多出一行「每类笔数：{'餐饮': 200xx, '零售': 199xx, ...}」，
#       这 5 个笔数相加应等于 100000；其余行不变。
#       要点：agg 传列表 = 一次算多个指标；count 只数非空值、size 数全部行，
#             我们的数据没有缺失值，所以此刻两者相同——如果换成有缺失的真实数据就会不同。
#
# 改完怎么验：重新跑一次脚本，看「改前改后输出差异点」是否与上面写的一致。


# ============================================================
# 第一层：脚手架（沫宁鱼写的，老大不用改，但建议读懂——答辩可能被问）
# ============================================================

def _timeit(func: Callable, rows: list[dict], repeat: int = REPEAT) -> tuple[float, dict]:
    """跑 repeat 次取中位数。第一次先跑一遍丢掉（预热），不计入结果。

    为什么预热：Python 第一次调用某函数要编译字节码、pandas 第一次 groupby 要
    分配内部缓存、CPU 还可能刚从节能频率升上来。这些一次性开销如果算进去，
    先跑的那个版本会凭空多出几十毫秒。所以先空跑一次再正式计时。
    为什么取中位数不是平均：中位数不受偶发卡顿（杀毒软件扫盘、系统调度）影响。
    """
    func(rows)  # 预热：让字节码编译 / 缓存分配 / CPU 升频先发生
    samples: list[float] = []
    result: dict = {}
    for _ in range(repeat):
        started = time.perf_counter()      # 单调高精度计时器，不受系统时间调整影响
        result = func(rows)
        samples.append(time.perf_counter() - started)
    return statistics.median(samples), result


def _timeit_pandas_parts(rows: list[dict], repeat: int = REPEAT) -> tuple[float, float, dict]:
    """把 Pandas 版拆成两段分别计时：建表（DataFrame 构造） vs 聚合（groupby.sum）。

    为什么要拆：Pandas 版的实际耗时 = 建表 + 聚合，两段成本差一个数量级。
    第一次实测发现总耗时反而比循环慢，就必须知道慢在哪一段，才能判断
    “是 Pandas 慢”还是“是我们的测法把建表成本算给了 Pandas”。

    两段的计时边界（公平性关键）：
      ① 建表：每次迭代都重新 pd.DataFrame(rows)，因为建表本来就是“每来一批数据做一次”的事；
      ② 聚合：表在**循环外只建一次**，循环里反复用同一张表聚合。
         —— 这样 ② 里绝不可能混进建表开销，“只比聚合”才是真的。
      ③ ②的返回值和循环版做 _check_same 校验，只比速度不比正确性等于比错了东西。
    """
    import pandas as pd

    # ---------- ① 只测建表 ----------
    pd.DataFrame(rows)  # 预热一次，同 _timeit
    build_samples: list[float] = []
    for _ in range(repeat):
        started = time.perf_counter()
        pd.DataFrame(rows)                                  # 只计时这一句
        build_samples.append(time.perf_counter() - started)

    # ---------- ② 只测聚合：表在循环外建好，循环里复用它 ----------
    frame = pd.DataFrame(rows)                              # 只建这一次，且不计入计时
    agg_samples: list[float] = []
    agg_result: dict = {}
    for _ in range(repeat):
        started = time.perf_counter()
        agg_result = frame.groupby(by="category_label")["consumption_amount"].sum().to_dict()
        agg_samples.append(time.perf_counter() - started)   # 计时区间只包含聚合 + to_dict()
    return (
        statistics.median(build_samples),
        statistics.median(agg_samples),
        agg_result,
    )


def _check_same(loop_result: dict, pandas_result: dict) -> None:
    """两版结果必须一致，差超过 0.01 元就算错（浮点累积误差）。"""
    if set(loop_result) != set(pandas_result):
        raise AssertionError(
            f"分类集合不一致：\n  loop={sorted(loop_result)}\n  pandas={sorted(pandas_result)}"
        )
    for key in loop_result:
        if abs(loop_result[key] - pandas_result[key]) > 0.01:
            raise AssertionError(
                f"分类 {key} 金额不一致：loop={loop_result[key]:.2f} pandas={pandas_result[key]:.2f}"
            )


def main() -> None:
    print(f"造数据 {ROWS:,} 行 ……")
    t0 = time.perf_counter()
    rows = build_test_rows(ROWS)
    print(f"造数据耗时 {time.perf_counter() - t0:.3f}s，实际 {len(rows):,} 行")
    print(f"样本一行：{rows[0]}\n")

    loop_time, loop_result = _timeit(group_sum_with_loop, rows)
    pandas_time, pandas_result = _timeit(group_sum_with_pandas, rows)

    _check_same(loop_result, pandas_result)

    ratio = loop_time / pandas_time
    print("=" * 56)
    print(f"循环版   耗时中位数：{loop_time * 1000:10.2f} ms")
    print(f"Pandas 版耗时中位数：{pandas_time * 1000:10.2f} ms")
    print(f"倍率 X = 循环 / Pandas = {ratio:.1f} 倍")
    print("=" * 56)
    print(f"分组数 {len(loop_result)}，两版结果已校验一致")

    build_ms, agg_ms, agg_result = _timeit_pandas_parts(rows)
    agg_ratio = loop_time / agg_ms if agg_ms else float("inf")

    # 分阶段那一路的聚合结果也要校验：只比速度不比正确性，等于比了个错的东西
    agg_same = True
    try:
        _check_same(loop_result, agg_result)
    except AssertionError as exc:
        agg_same = False
        print(f"\n[分阶段聚合结果校验失败] {exc}")

    print("\n--- 分阶段拆解：Pandas 版的时间花在哪 ---")
    print(f"① 建表  pd.DataFrame(rows)      ：{build_ms * 1000:10.2f} ms")
    print(f"② 聚合  groupby(...).sum()      ：{agg_ms * 1000:10.2f} ms")
    print(f"只比聚合：循环 / Pandas聚合 = {agg_ratio:.1f} 倍")
    print(f"聚合结果一致：{agg_same}")

    if ratio > 30:
        print("\n[注意] X > 30，先别写进预判卡：数字太夸张答辩容易被质疑造假。")
        print("  把两行耗时贴给沫宁鱼，换更公平的测法再测。")
    elif ratio < 1:
        print("\n[注意] X < 1，方向反了：Pandas 反而更慢。先别写进预判卡。")
        print("  看上面的分阶段拆解——如果 ① 建表远大于 ② 聚合，说明慢在建表，")
        print("  不是慢在聚合；把三行数字贴回对话，换更公平的测法。")
    else:
        print("\n[OK] X 在可用区间，把 X、两行耗时和三行分阶段数字贴回对话。")


if __name__ == "__main__":
    main()

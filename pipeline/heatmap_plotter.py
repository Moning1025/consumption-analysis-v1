# -*- coding: utf-8 -*-
"""
消费热力图生成器（Python matplotlib 版）
======================================
使用真实收集的数据生成空间热力图，输出为 PNG 图片。

运行方式：
    python pipeline/heatmap_plotter.py
"""
import json
import logging
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # 无界面后端，适合脚本运行
import matplotlib.pyplot as plt
import numpy as np

# 设置中文字体（Windows 环境）
plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "PingFang SC"]
plt.rcParams["axes.unicode_minus"] = False

logger = logging.getLogger("pipeline.heatmap_plotter")


def load_real_data(json_path: str = None) -> list:
    """从 JSON 文件加载真实热力图数据"""
    if json_path is None:
        json_path = Path(__file__).resolve().parent.parent / "web" / "heatmap_data.json"
    
    with open(json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload["data"]


def plot_spatial_heatmap(data: list, output_path: str = None):
    """
    绘制四川省消费空间热力图。
    
    使用散点图 + 颜色映射的方式模拟热力图效果。
    """
    if output_path is None:
        output_path = Path(__file__).resolve().parent.parent / "web" / "heatmap_map.png"
    
    # 提取坐标和热力值
    lngs = [d["lng"] for d in data]
    lats = [d["lat"] for d in data]
    heats = [d["heat_score"] for d in data]
    cities = [d["city"] for d in data]
    
    fig, ax = plt.subplots(figsize=(12, 10), dpi=150)
    
    # 绘制热力散点（大小和颜色都映射到热力值）
    scatter = ax.scatter(
        lngs, lats,
        s=[h * 8 for h in heats],  # 点大小
        c=heats,                    # 点颜色
        cmap="YlOrRd",              # 黄橙红色谱
        alpha=0.7,
        edgecolors="darkred",
        linewidth=0.5,
        zorder=5,
    )
    
    # 标注城市名称
    for i, city in enumerate(cities):
        ax.annotate(
            city,
            (lngs[i], lats[i]),
            textcoords="offset points",
            xytext=(8, 5),
            fontsize=9,
            fontweight="medium",
            color="#333",
            zorder=10,
        )
    
    # 设置标题和标签
    ax.set_title(
        "四川省城市消费热力分布图\n(数据来源：四川省统计局2023年统计公报)",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )
    ax.set_xlabel("经度 (°E)", fontsize=11)
    ax.set_ylabel("纬度 (°N)", fontsize=11)
    
    # 添加颜色条
    cbar = plt.colorbar(scatter, ax=ax, shrink=0.6, pad=0.02)
    cbar.set_label("消费热力指数", fontsize=10)
    
    # 设置网格
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_facecolor("#f8f9fa")
    
    # 自动调整范围
    lng_margin = (max(lngs) - min(lngs)) * 0.08
    lat_margin = (max(lats) - min(lats)) * 0.08
    ax.set_xlim(min(lngs) - lng_margin, max(lngs) + lng_margin)
    ax.set_ylim(min(lats) - lat_margin, max(lats) + lat_margin)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()
    
    logger.info("热力图已保存到: %s", output_path)
    print(f"✅ 热力图已生成: {output_path}")
    return str(output_path)


def plot_ranking_bar(data: list, output_path: str = None):
    """绘制城市消费排名条形图"""
    if output_path is None:
        output_path = Path(__file__).resolve().parent.parent / "web" / "heatmap_ranking.png"
    
    # 按热力值排序
    sorted_data = sorted(data, key=lambda x: x["heat_score"], reverse=True)
    cities = [d["city"] for d in sorted_data]
    heats = [d["heat_score"] for d in sorted_data]
    retails = [d.get("retail_total_yi", 0) for d in sorted_data]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 8), dpi=150)
    
    # 左图：热力指数排名
    colors = plt.cm.YlOrRd(np.array(heats) / max(heats))
    bars1 = ax1.barh(cities[::-1], heats[::-1], color=colors[::-1], edgecolor="darkred", linewidth=0.5)
    ax1.set_title("城市消费热力指数排名", fontsize=12, fontweight="bold")
    ax1.set_xlabel("热力指数", fontsize=10)
    ax1.grid(True, alpha=0.3, axis="x")
    
    # 在条形上标注数值
    for bar, val in zip(bars1, heats[::-1]):
        ax1.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                 f"{val:.1f}", va="center", fontsize=9)
    
    # 右图：社会消费品零售总额
    colors2 = plt.cm.Blues(np.array(retails) / max(retails))
    bars2 = ax2.barh(cities[::-1], retails[::-1], color=colors2[::-1], edgecolor="darkblue", linewidth=0.5)
    ax2.set_title("社会消费品零售总额（亿元）", fontsize=12, fontweight="bold")
    ax2.set_xlabel("零售总额（亿元）", fontsize=10)
    ax2.grid(True, alpha=0.3, axis="x")
    
    for bar, val in zip(bars2, retails[::-1]):
        ax2.text(bar.get_width() + 50, bar.get_y() + bar.get_height()/2,
                 f"{val:.0f}", va="center", fontsize=9)
    
    plt.suptitle("四川省各城市消费数据对比（2023年）", fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()
    
    logger.info("排名图已保存到: %s", output_path)
    print(f"✅ 排名图已生成: {output_path}")
    return str(output_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 60)
    print("消费热力图生成器")
    print("=" * 60)
    
    # 加载真实数据
    data = load_real_data()
    print(f"加载了 {len(data)} 个城市的真实数据")
    
    # 生成空间热力图
    plot_spatial_heatmap(data)
    
    # 生成排名对比图
    plot_ranking_bar(data)
    
    print("\n✅ 所有图表生成完成！")

# -*- coding: utf-8 -*-
# F&B 트렌드 생애주기 분석 시각화
# 실행: python3 src/analyze/visualize.py

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.font_manager as fm
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# --------------------------------------------------
# 폰트 설정 (macOS 기준)
# --------------------------------------------------
def set_korean_font():
    candidates = [
        "AppleGothic", "NanumGothic", "Malgun Gothic",
        "NanumBarunGothic", "Apple SD Gothic Neo"
    ]
    for name in candidates:
        if any(name in f.name for f in fm.fontManager.ttflist):
            plt.rcParams["font.family"] = name
            break
    plt.rcParams["axes.unicode_minus"] = False

set_korean_font()

# --------------------------------------------------
# 색상 팔레트 (남색 파스텔)
# --------------------------------------------------
C_SETTLE  = "#7BA3C8"   # 정착 — 부드러운 네이비 블루
C_FADE    = "#C8A0A0"   # 소멸 — 소프트 로즈
C_DARK    = "#3D5A80"   # 강조 네이비
C_LIGHT   = "#B8D4EA"   # 연한 블루
C_BG      = "#F4F7FB"   # 배경
C_GRID    = "#D8E4F0"   # 그리드
C_TEXT    = "#2C3E50"   # 텍스트

SETTLE_KEYWORDS = [
    "불닭볶음면", "소금빵", "뚱카롱", "크로플", "흑당밀크티",
    "포켓몬빵", "명랑핫도그", "허니버터칩", "요아정", "앙버터호두과자",
    "연세우유크림빵", "컵강정", "말차", "컵마라탕", "컵빙수", "버터떡"
]
FADE_KEYWORDS = [
    "탕후루", "대왕카스테라", "달고나커피", "마라탕", "버블티",
    "먹태깡", "크루키", "두바이초콜릿", "두바이쫀득쿠키", "로제떡볶이",
    "벌집아이스크림", "슈니발렌", "무한리필삼겹살", "이모카세", "소떡소떡",
    "오마카세", "쥬씨", "약과", "치즈등갈비", "시카고피자", "분모자",
    "컵냉면"
]

DATALAB_DIR    = "data/raw/datalab"
FEATURES_CSV   = "data/fnb_features_export.csv"
OUT_DIR        = "docs/figures"
os.makedirs(OUT_DIR, exist_ok=True)

def load_features():
    if not os.path.exists(FEATURES_CSV):
        return None
    return pd.read_csv(FEATURES_CSV)

def load_keyword(keyword):
    path = os.path.join(DATALAB_DIR, f"{keyword}.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, parse_dates=["date"])
    df = df.sort_values("date")
    # 월평균으로 스무딩
    df["ym"] = df["date"].dt.to_period("M")
    return df.groupby("ym")["ratio"].mean().reset_index()

def style_ax(ax, title):
    ax.set_facecolor(C_BG)
    ax.grid(axis="y", color=C_GRID, linewidth=0.8, linestyle="--")
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color(C_GRID)
    ax.tick_params(colors=C_TEXT, labelsize=9)
    ax.set_title(title, fontsize=12, fontweight="bold", color=C_DARK, pad=10)

# ==================================================
# 그림 1: 생애주기 곡선 — 불닭 vs 탕후루 vs 소금빵 vs 대왕카스테라
# ==================================================
def fig_lifecycle():
    targets = {
        "불닭볶음면": (C_SETTLE, "-",  "정착"),
        "소금빵":    (C_LIGHT,  "-",  "정착"),
        "탕후루":    (C_FADE,   "-",  "소멸"),
        "대왕카스테라": ("#E8B4B4", "-", "소멸"),
    }

    fig, ax = plt.subplots(figsize=(11, 5))
    fig.patch.set_facecolor(C_BG)

    for kw, (color, ls, label) in targets.items():
        df = load_keyword(kw)
        if df is None:
            continue
        xs = [str(p) for p in df["ym"]]
        ys = df["ratio"].values
        x_idx = np.arange(len(xs))
        ax.plot(x_idx, ys, color=color, linestyle=ls, linewidth=2.2, label=kw)
        # 피크 표시
        peak_i = np.argmax(ys)
        ax.scatter(x_idx[peak_i], ys[peak_i], color=color, s=60, zorder=5)

    # x축 연도만 표시
    df0 = load_keyword("불닭볶음면")
    xs_all = [str(p) for p in df0["ym"]]
    year_ticks = [i for i, x in enumerate(xs_all) if x.endswith("-01")]
    year_labels = [x[:4] for x in xs_all if x.endswith("-01")]
    ax.set_xticks(year_ticks)
    ax.set_xticklabels(year_labels, fontsize=9)

    style_ax(ax, "F&B 트렌드 생애주기 곡선")
    ax.set_ylabel("검색량 지수 (DataLab)", color=C_TEXT, fontsize=10)
    ax.legend(loc="upper left", framealpha=0.85, fontsize=9)

    # 정착/소멸 영역 레이블
    ax.text(0.98, 0.95, "정착형 — 피크 후에도 유지",
            transform=ax.transAxes, ha="right", va="top",
            fontsize=8.5, color=C_DARK, style="italic")
    ax.text(0.98, 0.87, "소멸형 — 피크 후 급락",
            transform=ax.transAxes, ha="right", va="top",
            fontsize=8.5, color="#C05050", style="italic")

    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/01_lifecycle.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("저장: 01_lifecycle.png")

# ==================================================
# 그림 2: spread_speed vs post_peak_avg 산점도
# ==================================================
def fig_scatter():
    df = load_features()
    if df is None:
        print("경고: fnb_features_export.csv 없음 — 02_scatter.png 건너뜀")
        return

    highlight = {"불닭볶음면", "탕후루", "소금빵", "대왕카스테라",
                 "쥬씨", "무한리필삼겹살", "요아정", "크루키"}

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor(C_BG)

    for _, row in df.iterrows():
        color = C_SETTLE if row["label"] == "정착" else C_FADE
        ax.scatter(row["spread_speed"], row["post_peak_avg"],
                   color=color, s=70, alpha=0.85, zorder=4)
        if row["keyword"] in highlight:
            offset = (-5, -12) if row["keyword"] == "무한리필삼겹살" else (3, 3)
            ax.annotate(row["keyword"],
                        (row["spread_speed"], row["post_peak_avg"]),
                        textcoords="offset points",
                        xytext=offset, fontsize=8, color=C_TEXT)

    settle_mean = df[df["label"] == "정착"]["spread_speed"].mean()
    fade_mean   = df[df["label"] == "소멸"]["spread_speed"].mean()
    n_s = (df["label"] == "정착").sum()
    n_f = (df["label"] == "소멸").sum()

    ax.axvline(x=settle_mean, color=C_SETTLE, linestyle="--", linewidth=1.2, alpha=0.7,
               label=f"정착 평균 spread={settle_mean:.2f}")
    ax.axvline(x=fade_mean,   color=C_FADE,   linestyle="--", linewidth=1.2, alpha=0.7,
               label=f"소멸 평균 spread={fade_mean:.2f}")

    patch_s = mpatches.Patch(color=C_SETTLE, label=f"정착 (n={n_s})")
    patch_f = mpatches.Patch(color=C_FADE,   label=f"소멸 (n={n_f})")

    style_ax(ax, "확산 속도 vs 지속성\n(급발진할수록 소멸, Cohen's d = -1.54)")
    ax.set_xlabel("확산속도 (spread_speed: 높을수록 급발진)", color=C_TEXT, fontsize=10)
    ax.set_ylabel("피크 후 평균 검색량 (post_peak_avg)", color=C_TEXT, fontsize=10)
    ax.legend(handles=[patch_s, patch_f], loc="upper right", fontsize=9)

    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/02_scatter.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("저장: 02_scatter.png")

# ==================================================
# 그림 3: 그룹별 주요 피처 비교 (정착 vs 소멸)
# ==================================================
def fig_feature_compare():
    features  = ["spread_speed", "volatility", "surge_count", "neg_ratio"]
    labels_kr = ["확산속도\n(↓정착)", "변동성\n(↑정착)", "재유행 횟수\n(↑정착)", "부정여론\n(차이 없음)"]

    df = load_features()
    if df is not None:
        grp    = df.groupby("label")[features].mean()
        settle = [grp.loc["정착", f] if "정착" in grp.index else 0 for f in features]
        fade   = [grp.loc["소멸", f] if "소멸" in grp.index else 0 for f in features]
    else:
        settle = [5.21,  9.43, 2.25, 0.282]
        fade   = [30.10, 5.11, 0.27, 0.308]

    x = np.arange(len(features))
    w = 0.35

    fig, ax = plt.subplots(figsize=(9, 5))
    fig.patch.set_facecolor(C_BG)

    bars_s = ax.bar(x - w/2, settle, w, color=C_SETTLE, label="정착 (n=22)",
                    alpha=0.9, zorder=3)
    bars_f = ax.bar(x + w/2, fade,   w, color=C_FADE,   label="소멸 (n=16)",
                    alpha=0.9, zorder=3)

    # 값 표시
    for bar in bars_s:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h + 0.3,
                f"{h:.2f}", ha="center", va="bottom", fontsize=8, color=C_DARK)
    for bar in bars_f:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h + 0.3,
                f"{h:.2f}", ha="center", va="bottom", fontsize=8, color="#8B4444")

    # 유의 표시 (|t|>2.03 ≈ p<0.05): spread_speed t=-4.47, volatility t=4.75 유의 /
    #   surge_count t=1.79, neg_ratio t=-2.00 유의하지 않음
    sig_map = {"spread_speed": "*", "volatility": "*",
               "surge_count": "n.s.", "neg_ratio": "n.s."}
    for i, feat in enumerate(features):
        ax.text(i, max(settle[i], fade[i]) + 1.5, sig_map[feat],
                ha="center", fontsize=11, color=C_DARK, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels(labels_kr, fontsize=9)
    style_ax(ax, "정착 vs 소멸 그룹별 피처 비교\n(* = 통계적 유의, n.s. = 유의하지 않음)")
    ax.set_ylabel("평균값", color=C_TEXT, fontsize=10)
    ax.legend(fontsize=9)

    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/03_feature_compare.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("저장: 03_feature_compare.png")

# ==================================================
# 그림 4: RF 피처 중요도
# ==================================================
def fig_importance():
    # 누수 없는 사전 지표만 (준-누수 volatility/surge_count/total_buzz/neg_ratio 제외)
    labels_kr = ["사전 인지도", "확산속도", "숏폼 시대"]
    scores    = [0.6370, 0.3385, 0.0244]
    colors    = [C_DARK, C_DARK, C_SETTLE]

    idx = np.argsort(scores)
    labels_sorted = [labels_kr[i] for i in idx]
    scores_sorted = [scores[i] for i in idx]
    colors_sorted = [colors[i] for i in idx]

    fig, ax = plt.subplots(figsize=(8, 4))
    fig.patch.set_facecolor(C_BG)

    bars = ax.barh(labels_sorted, scores_sorted,
                   color=colors_sorted, alpha=0.9, zorder=3, height=0.5)

    for bar, score in zip(bars, scores_sorted):
        ax.text(score + 0.005, bar.get_y() + bar.get_height()/2,
                f"{score:.4f}", va="center", fontsize=9, color=C_TEXT)

    style_ax(ax, "MLlib RandomForest 피처 중요도\n(CV 0.867 vs 기준선 0.579, n=38 해석용)")
    ax.set_xlabel("중요도 점수", color=C_TEXT, fontsize=10)
    ax.grid(axis="x", color=C_GRID, linewidth=0.8, linestyle="--")
    ax.grid(axis="y", visible=False)

    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/04_importance.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("저장: 04_importance.png")

# ==================================================
# 그림 5: 숏폼 전후 정착률 (신뢰 키워드 기준)
# ==================================================
def fig_shortform():
    eras   = ["숏폼 이전\n(~2020)", "숏폼 이후\n(2021~)"]
    settle = [7, 9]
    fade   = [8, 6]
    total  = [s + f for s, f in zip(settle, fade)]
    rate   = [s / t * 100 for s, t in zip(settle, total)]

    x = np.arange(len(eras))
    w = 0.5

    fig, axes = plt.subplots(1, 2, figsize=(10, 5))
    fig.patch.set_facecolor(C_BG)

    # 왼쪽: 누적 막대
    ax = axes[0]
    ax.bar(x, fade,   w, color=C_FADE,   label="소멸", alpha=0.9, zorder=3)
    ax.bar(x, settle, w, color=C_SETTLE, label="정착",
           bottom=fade, alpha=0.9, zorder=3)

    for i in range(len(eras)):
        ax.text(i, fade[i]/2, f"소멸\n{fade[i]}개",
                ha="center", va="center", fontsize=9, color="white", fontweight="bold")
        ax.text(i, fade[i] + settle[i]/2, f"정착\n{settle[i]}개",
                ha="center", va="center", fontsize=9, color="white", fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels(eras, fontsize=10)
    style_ax(ax, "숏폼 전후 정착/소멸 분포\n(신뢰 키워드 기준)")
    ax.set_ylabel("키워드 수", color=C_TEXT, fontsize=10)
    ax.legend(fontsize=9)

    # 오른쪽: 정착률
    ax2 = axes[1]
    bar_colors = [C_LIGHT, C_SETTLE]
    bars = ax2.bar(x, rate, w, color=bar_colors, alpha=0.9, zorder=3)
    for bar, r in zip(bars, rate):
        ax2.text(bar.get_x() + bar.get_width()/2,
                 bar.get_height() + 1.5,
                 f"{r:.0f}%", ha="center", fontsize=12,
                 fontweight="bold", color=C_DARK)

    ax2.set_xticks(x)
    ax2.set_xticklabels(eras, fontsize=10)
    ax2.set_ylim(0, 75)
    style_ax(ax2, "정착률 비교\n(recency 교란 존재 → 인과 불명확)")
    ax2.set_ylabel("정착률 (%)", color=C_TEXT, fontsize=10)

    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/05_shortform.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("저장: 05_shortform.png")

# ==================================================
# 그림 6: 피크 기준 전/후 분할 (분석 방법론 시각화)
#   왜 피크 기준으로 데이터를 나눴는가 = 누수 방지 핵심 설계
# ==================================================
def fig_peak_split():
    feats = load_features()
    if feats is None:
        print("경고: fnb_features_export.csv 없음 — 06_peak_split.png 건너뜀")
        return

    # 정착 대표(불닭볶음면) / 소멸 대표(탕후루)
    targets = [
        ("불닭볶음면", C_SETTLE, "피크 후 유지 → 정착"),
        ("탕후루",     C_FADE,   "피크 후 급락 → 소멸"),
    ]

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.patch.set_facecolor(C_BG)

    for ax, (kw, after_color, desc) in zip(axes, targets):
        df = load_keyword(kw)
        if df is None:
            continue
        x = df["ym"].dt.to_timestamp()
        y = df["ratio"].values

        # 피크 날짜는 분석 피처테이블 값을 그대로 사용 (시각화-분석 일치)
        row = feats[feats["keyword"] == kw]
        peak = pd.to_datetime(row["peak_date"].values[0])

        ax.plot(x, y, color=C_DARK, linewidth=2.0, zorder=4)

        # 피크 전(확산) / 후(정착·소멸) 음영
        x0, x1 = x.min(), x.max()
        ax.axvspan(x0, peak,  color=C_LIGHT,    alpha=0.45, zorder=1)
        ax.axvspan(peak, x1,  color=after_color, alpha=0.30, zorder=1)
        ax.axvline(peak, color=C_DARK, linestyle="--", linewidth=1.4, zorder=5)

        # 영역 레이블
        ymax = float(y.max())
        ax.text(x0 + (peak - x0) / 2, ymax * 0.95, "피크 이전\n(확산)",
                ha="center", va="top", fontsize=9, color=C_DARK, fontweight="bold")
        ax.text(peak + (x1 - peak) / 2, ymax * 0.95, "피크 이후",
                ha="center", va="top", fontsize=9, color=C_TEXT, fontweight="bold")
        ax.annotate("피크", (peak, ymax), textcoords="offset points",
                    xytext=(5, -2), fontsize=8, color=C_DARK)

        style_ax(ax, f"{kw} — {desc}")
        ax.set_ylabel("검색량 지수 (DataLab)", color=C_TEXT, fontsize=9)

    fig.suptitle("피크를 기준으로 '확산'과 '정착/소멸'을 분리해 분석",
                 fontsize=13, fontweight="bold", color=C_DARK, y=1.02)

    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/06_peak_split.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("저장: 06_peak_split.png")

# ==================================================
# 실행
# ==================================================
if __name__ == "__main__":
    print(f"그림 저장 경로: {OUT_DIR}/\n")
    fig_lifecycle()
    fig_scatter()
    fig_feature_compare()
    fig_importance()
    fig_shortform()
    fig_peak_split()
    print("\n시각화 완료!")

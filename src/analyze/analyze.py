# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, when, lit,
    months_between, greatest,
    corr, round as spark_round,
    year as spark_year
)
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("FnbTrendAnalysis") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/apps/spark/warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

labels  = spark.table("fnb_labels")
datalab = spark.table("fnb_datalab")
news    = spark.table("fnb_news")
blog    = spark.table("fnb_blog")
youtube = spark.table("fnb_youtube")

# --------------------------------------------------
# Q1: 확산속도 vs 지속성
# 확산속도 = 100 / max(피크 직전 1개월 평균, 1.0)
#   → 직전 1개월이 낮을수록(갑작스러울수록) 높은 값
#   ※ 피크가 데이터 시작점에 있어 직전 1개월 데이터가 없으면 NULL
#     (fillna로 100을 주면 "데이터 없음"이 "최고속도"로 둔갑하므로 제외)
# 지속성  = post_peak_avg (fnb_labels에서 그대로 사용)
# --------------------------------------------------
print("\n" + "="*60)
print("Q1: 확산속도 vs 지속성")
print("="*60)

pre1m_df = datalab.alias("d") \
    .join(labels.select("keyword", "peak_date").alias("l"), "keyword") \
    .filter(
        (col("d.date") < col("l.peak_date")) &
        (months_between(col("l.peak_date"), col("d.date")) <= 1)
    ) \
    .groupBy("keyword") \
    .agg(avg("d.ratio").alias("pre_1m_avg"))

q1_df = labels \
    .join(pre1m_df, "keyword", "left") \
    .withColumn(
        # 직전 1개월 데이터가 있을 때만 산출, 없으면 NULL (아티팩트 방지)
        "spread_speed",
        F.when(
            col("pre_1m_avg").isNotNull(),
            spark_round(lit(100.0) / greatest(col("pre_1m_avg"), lit(1.0)), 2)
        ).otherwise(lit(None).cast("double"))
    )

print("\n[키워드별 확산속도 vs 지속성]")
q1_df.select(
    "keyword", "label", "reliable",
    "spread_speed",
    spark_round("post_peak_avg", 2).alias("post_peak_avg")
).orderBy(col("spread_speed").isNull(), col("spread_speed").desc()).show(40, truncate=False)

print("[Q1 상관계수: 확산속도 vs 지속성(post_peak_avg)]")
print("- 전체 (spread_speed NULL 자동 제외)")
q1_df.agg(spark_round(corr("spread_speed", "post_peak_avg"), 4).alias("corr_전체")).show()
print("- 신뢰 키워드만 (reliable=True)")
q1_df.filter(col("reliable")).agg(
    spark_round(corr("spread_speed", "post_peak_avg"), 4).alias("corr_신뢰")
).show()

print("[Q1 정착/소멸 그룹별 평균]")
q1_df.groupBy("label").agg(
    spark_round(avg("spread_speed"),   2).alias("avg_spread_speed"),
    spark_round(avg("post_peak_avg"),  2).alias("avg_post_peak_avg"),
    count("*").alias("n")
).show()

# --------------------------------------------------
# Q2: 부정여론 비율 vs 쇠퇴속도
# 부정여론  = 각 소스별 sentiment<0 비율 단순평균
# 쇠퇴속도  = 1 - (post_peak_avg / 100)  → 0=유지, 1=완전소멸
# --------------------------------------------------
print("\n" + "="*60)
print("Q2: 부정여론 비율 vs 쇠퇴속도")
print("="*60)

def src_neg_ratio(df, name):
    return df.groupBy("keyword").agg(
        count(when(col("sentiment") < 0, 1)).alias("neg"),
        count(when(col("sentiment").isNotNull(), 1)).alias("total")
    ).withColumn(f"neg_{name}", spark_round(col("neg") / greatest(col("total"), lit(1)), 4)) \
     .select("keyword", f"neg_{name}")

neg_df = src_neg_ratio(news, "news") \
    .join(src_neg_ratio(blog, "blog"),    "keyword", "left") \
    .join(src_neg_ratio(youtube, "yt"),   "keyword", "left") \
    .fillna(0.0) \
    .withColumn(
        "neg_ratio",
        spark_round(
            (col("neg_news") + col("neg_blog") + col("neg_yt")) / lit(3.0), 4
        )
    )

q2_df = labels \
    .join(neg_df, "keyword", "left") \
    .fillna({"neg_ratio": 0.0}) \
    .withColumn(
        "decay_speed",
        spark_round(lit(1.0) - (col("post_peak_avg") / lit(100.0)), 4)
    )

print("\n[키워드별 부정여론 비율 vs 쇠퇴속도]")
q2_df.select(
    "keyword", "label", "reliable", "neg_ratio", "decay_speed"
).orderBy(col("neg_ratio").desc()).show(40, truncate=False)

print("[Q2 상관계수: 부정여론 비율 vs 쇠퇴속도]")
print("- 전체")
q2_df.agg(spark_round(corr("neg_ratio", "decay_speed"), 4).alias("corr_전체")).show()
print("- 신뢰 키워드만")
q2_df.filter(col("reliable")).agg(
    spark_round(corr("neg_ratio", "decay_speed"), 4).alias("corr_신뢰")
).show()

print("[Q2 정착/소멸 그룹별 평균 부정여론 비율]")
q2_df.groupBy("label").agg(
    spark_round(avg("neg_ratio"),   4).alias("avg_neg_ratio"),
    spark_round(avg("decay_speed"), 4).alias("avg_decay_speed"),
    count("*").alias("n")
).show()

# --------------------------------------------------
# Q3: 정착/소멸 분류 피처 비교
# 피처: spread_speed, neg_ratio, pre_peak_avg, shortform_era
# 38개 샘플 → MLlib 과적합 위험 → 그룹 평균 비교
# 숏폼 기준: peak_date year >= 2021 (국내 숏폼 대중화 시점)
# --------------------------------------------------
print("\n" + "="*60)
print("Q3: 정착/소멸 피처 비교")
print("="*60)

q3_df = q1_df \
    .join(neg_df.select("keyword", "neg_ratio"), "keyword", "left") \
    .fillna({"neg_ratio": 0.0}) \
    .withColumn(
        "shortform_era",
        when(spark_year(col("peak_date")) >= 2021, lit(1)).otherwise(lit(0))
    )

print("\n[Q3 정착/소멸 그룹별 피처 평균]")
q3_df.groupBy("label").agg(
    spark_round(avg("spread_speed"),  2).alias("avg_spread_speed"),
    spark_round(avg("pre_peak_avg"),  2).alias("avg_pre_peak"),
    spark_round(avg("post_peak_avg"), 2).alias("avg_post_peak"),
    spark_round(avg("neg_ratio"),     4).alias("avg_neg_ratio"),
    spark_round(avg("shortform_era"), 4).alias("shortform_ratio"),
    count("*").alias("n")
).show()

print("\n[Q3 숏폼 전/후 × 정착/소멸 분포]")
q3_df.groupBy("shortform_era", "label").count() \
    .orderBy("shortform_era", "label").show()

# 최신성 교란 방어: 2021년 이후 키워드는 아직 쇠퇴할 시간이 없어 정착 과대 → 신뢰 라벨만 재집계
print("\n[Q3 숏폼 전/후 × 정착/소멸 — 신뢰 키워드만(recency 교란 제거)]")
q3_df.filter(col("reliable")).groupBy("shortform_era", "label").count() \
    .orderBy("shortform_era", "label").show()

print("\n[Q3 숏폼 전/후 그룹별 평균 지속성]")
q3_df.groupBy("shortform_era").agg(
    spark_round(avg("post_peak_avg"), 2).alias("avg_post_peak"),
    spark_round(avg("neg_ratio"),     4).alias("avg_neg_ratio"),
    count("*").alias("n")
).orderBy("shortform_era").show()

# 결과 저장
print("\n=== 분석 결과 저장 ===")
for df, name in [(q1_df, "fnb_q1"), (q2_df, "fnb_q2"), (q3_df, "fnb_q3")]:
    spark.sql(f"DROP TABLE IF EXISTS {name}_tmp")
    df.write.mode("overwrite").saveAsTable(f"{name}_tmp")
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.sql(f"ALTER TABLE {name}_tmp RENAME TO {name}")
    print(f"{name} 저장 완료")

spark.stop()
print("\n분석 완료!")

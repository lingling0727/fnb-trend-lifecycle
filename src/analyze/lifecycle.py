# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, when, lit,
    min as spark_min, datediff,
    round as spark_round,
    year as spark_year, corr
)
import pyspark.sql.functions as F
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder \
    .appName("FnbLifecycle") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/apps/spark/warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ratio >= 20 = "뜨기 시작" / < 20 = "사실상 소멸"
# peak은 항상 100이므로 20 = 피크의 20%
THRESHOLD = 20.0
# rise_start가 데이터 시작 후 이 일수 이내면 "데이터 시작 시점에 이미 유행 중"
# = 실제 부상 시점이 2016년 이전 → rise_days 신뢰 불가
BOUNDARY_DAYS = 30

labels  = spark.table("fnb_labels")
datalab = spark.table("fnb_datalab")

# 키워드별 데이터 시작일 (경계 아티팩트 판정용)
bounds_df = datalab.groupBy("keyword").agg(spark_min("date").alias("data_min"))

# --------------------------------------------------
# 피크까지 도달 기간 (rise_days)
# ratio >= THRESHOLD를 처음 넘은 날 → peak_date까지 일수
# 단, rise_start가 데이터 시작 직후면 2016년 이전부터 유행한 것 → NULL 처리
# --------------------------------------------------
rise_df = datalab.alias("d") \
    .join(labels.select("keyword", "peak_date").alias("l"), "keyword") \
    .filter(
        (col("d.date") < col("l.peak_date")) &
        (col("d.ratio") >= THRESHOLD)
    ) \
    .groupBy("keyword") \
    .agg(spark_min("d.date").alias("rise_start"))

rise_time_df = labels.select("keyword", "peak_date", "label", "reliable") \
    .join(bounds_df, "keyword") \
    .join(rise_df, "keyword", "left") \
    .withColumn("rise_days_raw", datediff(col("peak_date"), col("rise_start"))) \
    .withColumn(
        # 데이터 시작 직후부터 이미 임계값 이상이면 실제 부상은 데이터 밖 → 신뢰 불가
        "rise_boundary",
        col("rise_start").isNotNull() &
        (datediff(col("rise_start"), col("data_min")) <= BOUNDARY_DAYS)
    ) \
    .withColumn(
        "rise_days",
        F.when(col("rise_boundary"), lit(None).cast("int")).otherwise(col("rise_days_raw"))
    )

# --------------------------------------------------
# 소멸까지 기간 (fall_days)
# peak_date 이후 ratio < THRESHOLD로 처음 떨어진 날까지 일수
# null = 아직 소멸 안 됨 (정착형)
# --------------------------------------------------
fall_df = datalab.alias("d") \
    .join(labels.select("keyword", "peak_date").alias("l"), "keyword") \
    .filter(
        (col("d.date") > col("l.peak_date")) &
        (col("d.ratio") < THRESHOLD)
    ) \
    .groupBy("keyword") \
    .agg(spark_min("d.date").alias("fall_start"))

fall_time_df = labels.select("keyword", "peak_date") \
    .join(fall_df, "keyword", "left") \
    .withColumn("fall_days", datediff(col("fall_start"), col("peak_date")))

# --------------------------------------------------
# 통합
# --------------------------------------------------
lifecycle_df = rise_time_df \
    .join(fall_time_df.select("keyword", "fall_days"), "keyword", "left") \
    .withColumn(
        "shortform_era",
        when(spark_year(col("peak_date")) >= 2021, lit(1)).otherwise(lit(0))
    )

# --------------------------------------------------
# 출력 1: 키워드별 생애주기 기간
# --------------------------------------------------
print("\n" + "="*60)
print("키워드별 생애주기 기간")
print(f"기준: ratio >= {THRESHOLD} 진입 → 피크 → ratio < {THRESHOLD} 이탈")
print(f"rise_boundary=True: 2016년 이전부터 유행 → rise_days 신뢰 불가(NULL)")
print("="*60)

lifecycle_df.select(
    "keyword", "label", "peak_date",
    "rise_days", "rise_boundary", "fall_days"
).orderBy(col("rise_days").isNull(), col("rise_days").asc()).show(40, truncate=False)

# --------------------------------------------------
# 출력 2: 정착/소멸 그룹별 평균 기간 (NULL 자동 제외)
# --------------------------------------------------
print("\n[정착/소멸 그룹별 기간 — 평균 + 중앙값]")
# 평균은 말차(rise 3472일) 같은 극단 아티팩트에 취약 → 중앙값 병기
lifecycle_df.groupBy("label").agg(
    spark_round(avg("rise_days"), 1).alias("avg_rise"),
    spark_round(F.expr("percentile_approx(rise_days, 0.5)"), 1).alias("med_rise"),
    spark_round(avg("fall_days"), 1).alias("avg_fall"),
    spark_round(F.expr("percentile_approx(fall_days, 0.5)"), 1).alias("med_fall"),
    count("rise_days").alias("rise_유효"),
    count("fall_days").alias("fall_유효"),
    count("*").alias("n")
).show()

# --------------------------------------------------
# 출력 3: 확산 빠를수록 소멸도 빠른가? (상관관계)
# rise_days, fall_days 둘 다 있는 키워드만 자동 사용
# Pearson은 말차(rise 3472일) 같은 극단 아웃라이어에 취약 →
#   특정 점을 손으로 빼지 않고(체리피킹 방지) 순위기반 Spearman을 병기.
#   Spearman은 모든 아웃라이어의 영향을 자동으로 완화함.
# --------------------------------------------------
print("\n[상관관계: rise_days vs fall_days]")
print("(음수 상관 = 빠른 확산일수록 빠른 소멸)")
print("Pearson=아웃라이어 취약 / Spearman=순위기반 강건")

corr_src = lifecycle_df.select(
    col("rise_days").cast("double").alias("rise_days"),
    col("fall_days").cast("double").alias("fall_days")
).dropna()

corr_vec = VectorAssembler(
    inputCols=["rise_days", "fall_days"], outputCol="v"
).transform(corr_src)

pear  = Correlation.corr(corr_vec, "v", "pearson").head()[0].toArray()[0][1]
spear = Correlation.corr(corr_vec, "v", "spearman").head()[0].toArray()[0][1]
print("  Pearson  : {:.4f}  (말차 아웃라이어에 끌려간 값)".format(pear))
print("  Spearman : {:.4f}  (순위기반 = 실제 관계에 가까움)".format(spear))
print("  (유효 표본 n = {})".format(corr_src.count()))

# --------------------------------------------------
# 출력 4: 숏폼 이전/이후 피크 도달 속도 비교
# --------------------------------------------------
print("\n" + "="*60)
print("숏폼 이전/이후 피크 도달 속도 비교")
print("(0 = 2021년 이전, 1 = 2021년 이후 / rise_days NULL 제외)")
print("="*60)

lifecycle_df.groupBy("shortform_era").agg(
    spark_round(avg("rise_days"), 1).alias("avg_rise_days"),
    spark_round(avg("fall_days"), 1).alias("avg_fall_days"),
    count("rise_days").alias("rise_유효"),
    count("*").alias("n")
).orderBy("shortform_era").show()

print("\n[숏폼 × 정착/소멸 교차]")
lifecycle_df.groupBy("shortform_era", "label").agg(
    spark_round(avg("rise_days"), 1).alias("avg_rise_days"),
    spark_round(avg("fall_days"), 1).alias("avg_fall_days"),
    count("*").alias("n")
).orderBy("shortform_era", "label").show()

# 최신성 교란 방어: reliable=True(피크 후 12개월 확보)만으로 재집계
print("\n[숏폼 × 정착/소멸 교차 — 신뢰 키워드만(recency 교란 제거)]")
lifecycle_df.filter(col("reliable")).groupBy("shortform_era", "label") \
    .count().orderBy("shortform_era", "label").show()

# 저장
print("\n=== fnb_lifecycle 저장 ===")
spark.sql("DROP TABLE IF EXISTS fnb_lifecycle_tmp")
lifecycle_df.write.mode("overwrite").saveAsTable("fnb_lifecycle_tmp")
spark.sql("DROP TABLE IF EXISTS fnb_lifecycle")
spark.sql("ALTER TABLE fnb_lifecycle_tmp RENAME TO fnb_lifecycle")
print("fnb_lifecycle 저장 완료")

spark.stop()
print("\n생애주기 분석 완료!")

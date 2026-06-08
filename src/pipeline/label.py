# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, lit, months_between, row_number, greatest, datediff,
    min as spark_min, max as spark_max, round as spark_round
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("FnbTrendLabel") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/apps/spark/warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 임계값
RETENTION_THRESHOLD = 0.5   # post_peak_avg / pre_peak_avg >= 0.5
ABS_THRESHOLD       = 10.0  # post_peak_avg >= 10 (DataLab 100점 스케일 기준)
PRE_PEAK_FLOOR      = 1.0   # 갑작스럽게 등장한 키워드의 pre_peak=0 보정
SMOOTH_DAYS         = 15    # 피크 탐지용 이동평균 반경(±15일 = 31일 중심)
PEAK_FRAC           = 0.8   # 첫 주요 피크 기준: 평활 최댓값의 80% 첫 도달 시점
MIN_POST_MONTHS     = 12.0  # 피크 이후 최소 12개월 데이터가 있어야 라벨 신뢰
MIN_PRE_MONTHS      = 1.0   # 피크 이전 최소 1개월 데이터가 있어야 라벨 신뢰

print("=== [1] fnb_datalab 로드 ===")
datalab = spark.table("fnb_datalab")
print(f"총 {datalab.count()}건")

# 키워드별 데이터 수집 범위 (충분성 판단용)
bounds_df = datalab.groupBy("keyword").agg(
    spark_min("date").alias("data_min"),
    spark_max("date").alias("data_max")
)

# 키워드별 피크 날짜 + ratio (Spark 2.x 호환 Window 방식)
# 피크 정의 = '첫 주요 피크'(상승의 끝):
#   1) 단발 이상치(DataLab 하루짜리 글리치) 방어 위해 31일 중심 이동평균으로 평활
#   2) 평활값이 키워드별 최댓값의 PEAK_FRAC(80%)에 처음 도달한 날을 피크로 지정
#   - 일별 argmax는 하루 스파이크에 속음 (예: 탕후루 2019 글리치 vs 2023 바이럴)
#   - 글로벌 최댓값은 다파동 정착 제품(불닭)이 최신 파동을 피크로 잡아 post 구간을 왜곡
#   - 첫 주요 피크는 '상승→피크→정착/소멸' 생애주기 정의에 부합
print("\n=== [2] 키워드별 피크 계산 (31일 이동평균 + 첫 주요 피크) ===")
# 날짜를 일수로 변환 (rangeBetween용)
dl = datalab.withColumn("day_num", datediff(col("date"), lit("2016-01-01")).cast("long"))
# ±SMOOTH_DAYS(31일 중심) 이동평균
w_roll = Window.partitionBy("keyword").orderBy("day_num").rangeBetween(-SMOOTH_DAYS, SMOOTH_DAYS)
dl = dl.withColumn("ratio_smooth", avg("ratio").over(w_roll))
# 키워드별 평활 최댓값
smax_df = dl.groupBy("keyword").agg(spark_max("ratio_smooth").alias("smax"))
# 평활값이 최댓값의 80% 이상이 되는 '첫' 날 = 첫 주요 피크
w = Window.partitionBy("keyword").orderBy(col("date").asc())
peak_df = dl.join(smax_df, "keyword") \
    .filter(col("ratio_smooth") >= col("smax") * lit(PEAK_FRAC)) \
    .withColumn("rn", row_number().over(w)) \
    .filter(col("rn") == 1) \
    .select(
        col("keyword"),
        col("ratio").alias("peak_ratio"),
        col("date").alias("peak_date")
    )

# 피크 이전 12개월 평균
print("\n=== [3] 피크 이전 12개월 평균 계산 ===")
pre_peak_df = datalab.alias("d").join(peak_df.alias("p"), "keyword") \
    .filter(
        (col("d.date") < col("p.peak_date")) &
        (months_between(col("p.peak_date"), col("d.date")) <= 12)
    ) \
    .groupBy("keyword") \
    .agg(avg("d.ratio").alias("pre_peak_avg"))

# 피크 이후 12개월 평균
print("\n=== [4] 피크 이후 12개월 평균 계산 ===")
post_peak_df = datalab.alias("d").join(peak_df.alias("p"), "keyword") \
    .filter(
        (col("d.date") > col("p.peak_date")) &
        (months_between(col("d.date"), col("p.peak_date")) <= 12)
    ) \
    .groupBy("keyword") \
    .agg(avg("d.ratio").alias("post_peak_avg"))

# 결합 및 라벨 부여
print("\n=== [5] 지속률 계산 및 라벨 부여 ===")
label_df = peak_df \
    .join(bounds_df, "keyword") \
    .join(pre_peak_df,  "keyword", "left") \
    .join(post_peak_df, "keyword", "left") \
    .fillna({"pre_peak_avg": 0.0, "post_peak_avg": 0.0}) \
    .withColumn(
        "retention_ratio",
        spark_round(
            col("post_peak_avg") / greatest(col("pre_peak_avg"), lit(PRE_PEAK_FLOOR)),
            4
        )
    ) \
    .withColumn(
        "label",
        F.when(
            (col("retention_ratio") >= RETENTION_THRESHOLD) &
            (col("post_peak_avg")   >= ABS_THRESHOLD),
            "정착"
        ).otherwise("소멸")
    ) \
    .withColumn(
        # 피크 기준 실제로 확보된 전/후 데이터 개월 수
        "post_months_avail",
        spark_round(months_between(col("data_max"), col("peak_date")), 1)
    ) \
    .withColumn(
        "pre_months_avail",
        spark_round(months_between(col("peak_date"), col("data_min")), 1)
    ) \
    .withColumn(
        # 라벨 신뢰 여부: 피크 후 12개월 + 피크 전 1개월 데이터 확보 시에만 True
        "reliable",
        (col("post_months_avail") >= MIN_POST_MONTHS) &
        (col("pre_months_avail")  >= MIN_PRE_MONTHS)
    )

# 결과 출력
print(f"\n임계값: retention>={RETENTION_THRESHOLD}, post_peak_avg>={ABS_THRESHOLD}")
print(f"신뢰 조건: 피크 후 >={MIN_POST_MONTHS}개월 AND 피크 전 >={MIN_PRE_MONTHS}개월 데이터")
label_df.select(
    "keyword", "peak_date", "label",
    spark_round("pre_peak_avg",  2).alias("pre_avg"),
    spark_round("post_peak_avg", 2).alias("post_avg"),
    "retention_ratio",
    "post_months_avail", "reliable"
).orderBy("retention_ratio", ascending=False).show(50, truncate=False)

print("\n=== 라벨 분포 (전체) ===")
label_df.groupBy("label").count().show()

print("=== 라벨 분포 (신뢰 가능한 것만) ===")
label_df.filter(col("reliable")).groupBy("label").count().show()

print("=== 데이터 부족으로 신뢰 불가한 키워드 (라벨 해석 주의) ===")
label_df.filter(~col("reliable")).select(
    "keyword", "peak_date", "label",
    "post_months_avail", "pre_months_avail"
).orderBy("post_months_avail").show(20, truncate=False)

# 저장
print("\n=== [6] fnb_labels 저장 ===")
spark.sql("DROP TABLE IF EXISTS fnb_labels_tmp")
label_df.write.mode("overwrite").saveAsTable("fnb_labels_tmp")
spark.sql("DROP TABLE IF EXISTS fnb_labels")
spark.sql("ALTER TABLE fnb_labels_tmp RENAME TO fnb_labels")
print("fnb_labels 저장 완료")

spark.stop()
print("\n라벨링 완료!")

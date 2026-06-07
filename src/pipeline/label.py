# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, lit, months_between, row_number, greatest
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

print("=== [1] fnb_datalab 로드 ===")
datalab = spark.table("fnb_datalab")
print(f"총 {datalab.count()}건")

# 키워드별 피크 날짜 + ratio (Spark 2.x 호환 Window 방식)
print("\n=== [2] 키워드별 피크 계산 ===")
w = Window.partitionBy("keyword").orderBy(col("ratio").desc(), col("date").desc())
peak_df = datalab.withColumn("rn", row_number().over(w)) \
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
    .join(pre_peak_df,  "keyword", "left") \
    .join(post_peak_df, "keyword", "left") \
    .fillna({"pre_peak_avg": 0.0, "post_peak_avg": 0.0}) \
    .withColumn(
        "retention_ratio",
        F.round(
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
    )

# 결과 출력
print(f"\n임계값: retention>={RETENTION_THRESHOLD}, post_peak_avg>={ABS_THRESHOLD}")
label_df.select(
    "keyword", "peak_date", "peak_ratio",
    F.round("pre_peak_avg",  2).alias("pre_peak_avg"),
    F.round("post_peak_avg", 2).alias("post_peak_avg"),
    "retention_ratio", "label"
).orderBy("retention_ratio", ascending=False).show(50, truncate=False)

print("\n=== 라벨 분포 ===")
label_df.groupBy("label").count().show()

# 저장
print("\n=== [6] fnb_labels 저장 ===")
spark.sql("DROP TABLE IF EXISTS fnb_labels_tmp")
label_df.write.mode("overwrite").saveAsTable("fnb_labels_tmp")
spark.sql("DROP TABLE IF EXISTS fnb_labels")
spark.sql("ALTER TABLE fnb_labels_tmp RENAME TO fnb_labels")
print("fnb_labels 저장 완료")

spark.stop()
print("\n라벨링 완료!")

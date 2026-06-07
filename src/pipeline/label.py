# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, max as spark_max, avg, count, lit,
    months_between, row_number
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("FnbTrendLabel") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/apps/spark/warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 임계값: 피크 이후 12개월 평균 / 피크 >= 이 값이면 정착
PERSISTENCE_THRESHOLD = 0.3

print("=== [1] fnb_datalab 로드 ===")
datalab = spark.table("fnb_datalab")
datalab.printSchema()
print(f"총 {datalab.count()}건")

# 키워드별 피크 ratio + 피크 날짜 (Spark 2.x 호환 — Window 방식)
print("\n=== [2] 키워드별 피크 계산 ===")
w = Window.partitionBy("keyword").orderBy(col("ratio").desc(), col("date").desc())
peak_df = datalab.withColumn("rn", row_number().over(w)) \
    .filter(col("rn") == 1) \
    .select(
        col("keyword"),
        col("ratio").alias("peak_ratio"),
        col("date").alias("peak_date")
    )

total_df = datalab.groupBy("keyword").agg(count("*").alias("total_rows"))
peak_df = peak_df.join(total_df, "keyword")

# 피크 이후 12개월 데이터와 join
print("\n=== [3] 피크 이후 12개월 평균 계산 ===")
joined = datalab.alias("d").join(peak_df.alias("p"), "keyword") \
    .filter(
        (col("d.date") > col("p.peak_date")) &
        (months_between(col("d.date"), col("p.peak_date")) <= 12)
    )

post_peak_df = joined.groupBy("keyword").agg(
    avg("d.ratio").alias("post_peak_avg")
)

# 지속률 계산 및 라벨 부여
print("\n=== [4] 지속률 계산 및 라벨 부여 ===")
label_df = peak_df.join(post_peak_df, "keyword", "left") \
    .fillna({"post_peak_avg": 0.0}) \
    .withColumn(
        "persistence_ratio",
        F.round(col("post_peak_avg") / col("peak_ratio"), 4)
    ) \
    .withColumn(
        "label",
        F.when(col("persistence_ratio") >= PERSISTENCE_THRESHOLD, "정착")
         .otherwise("소멸")
    ) \
    .withColumn("threshold", lit(PERSISTENCE_THRESHOLD))

# 결과 출력
print(f"\n임계값: {PERSISTENCE_THRESHOLD} (피크 이후 12개월 평균 / 피크)")
label_df.select(
    "keyword", "peak_ratio", "peak_date",
    "post_peak_avg", "persistence_ratio", "label"
).orderBy("persistence_ratio", ascending=False).show(50, truncate=False)

# 정착/소멸 분포
print("\n=== 라벨 분포 ===")
label_df.groupBy("label").count().show()

# Hive 저장
print("\n=== [5] fnb_labels 저장 ===")
spark.sql("DROP TABLE IF EXISTS fnb_labels_tmp")
label_df.write.mode("overwrite").saveAsTable("fnb_labels_tmp")
spark.sql("DROP TABLE IF EXISTS fnb_labels")
spark.sql("ALTER TABLE fnb_labels_tmp RENAME TO fnb_labels")
print("fnb_labels 저장 완료")

spark.stop()
print("\n라벨링 완료!")

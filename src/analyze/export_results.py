# -*- coding: utf-8 -*-
# HDP 분석 결과를 로컬 CSV로 추출
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FnbExport") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/apps/spark/warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# fnb_features + fnb_labels 조인해서 필요한 컬럼 추출
feat = spark.table("fnb_features")
feat.select(
    "keyword", "label", "reliable", "peak_date",
    "spread_speed", "volatility", "surge_count",
    "total_buzz", "neg_ratio", "shortform_era",
    "pre_peak_avg", "post_peak_avg", "retention_ratio"
).coalesce(1).write.mode("overwrite").option("header", True) \
 .csv("hdfs:///user/maria_dev/fnb_export")

print("HDFS 저장 완료: hdfs:///user/maria_dev/fnb_export")
spark.stop()

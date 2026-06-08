# -*- coding: utf-8 -*-
# 키워드별 피처 마스터 테이블 생성 (fnb_features)
# - fnb_labels(피크/전후평균/라벨) + DataLab 파생 + 버즈량 + 부정여론을 1행으로 통합
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, lit, when, greatest,
    stddev, datediff, months_between, lag,
    covar_pop, var_pop, sum as spark_sum,
    year as spark_year, round as spark_round
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("FnbFeatures") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/apps/spark/warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 월평균 ratio가 피크의 30% 이상으로 올라온 1회 = 관심 급증(surge) 1회
SURGE_LEVEL = 30.0

labels  = spark.table("fnb_labels")
datalab = spark.table("fnb_datalab")
news    = spark.table("fnb_news")
blog    = spark.table("fnb_blog")
youtube = spark.table("fnb_youtube")

# --------------------------------------------------
# 1. 확산속도 = 100 / max(피크 직전 1개월 평균, 1)
#    직전 1개월 데이터 없으면 NULL (아티팩트 방지)
# --------------------------------------------------
pre1m = datalab.alias("d") \
    .join(labels.select("keyword", "peak_date").alias("l"), "keyword") \
    .filter(
        (col("d.date") < col("l.peak_date")) &
        (months_between(col("l.peak_date"), col("d.date")) <= 1)
    ) \
    .groupBy("keyword").agg(avg("d.ratio").alias("pre_1m_avg"))

# --------------------------------------------------
# 2. 변동성 = 전 기간 ratio 표준편차
# --------------------------------------------------
vol = datalab.groupBy("keyword").agg(spark_round(stddev("ratio"), 2).alias("volatility"))

# --------------------------------------------------
# 3. 쇠퇴 기울기 = 피크 후 12개월 OLS slope = cov(day, ratio) / var(day)
#    음수일수록 빠르게 하락. (decay_speed=낙폭과 달리 '하루당 변화율'=진짜 속도)
# --------------------------------------------------
post = datalab.alias("d") \
    .join(labels.select("keyword", "peak_date").alias("l"), "keyword") \
    .filter(
        (col("d.date") > col("l.peak_date")) &
        (months_between(col("d.date"), col("l.peak_date")) <= 12)
    ) \
    .withColumn("day_idx", datediff(col("d.date"), col("l.peak_date")))

slope = post.groupBy("keyword").agg(
    covar_pop("day_idx", "ratio").alias("cov"),
    var_pop("day_idx").alias("varx"),
    count("*").alias("npts")
).withColumn(
    "decay_slope",
    when(
        (col("varx") > 0) & (col("npts") >= 3),
        spark_round(col("cov") / col("varx"), 4)
    ).otherwise(lit(None).cast("double"))
).select("keyword", "decay_slope")

# --------------------------------------------------
# 4. 재유행(surge_count) = 월평균 ratio가 아래→위로 SURGE_LEVEL을 넘은 횟수
#    1 = 한 번 떴다 끝 / 2+ = 다시 떠오른 적 있음(재유행성)
# --------------------------------------------------
monthly = datalab.groupBy("keyword", "year", "month") \
    .agg(avg("ratio").alias("m_ratio")) \
    .withColumn("ym", col("year") * 100 + col("month"))

wm = Window.partitionBy("keyword").orderBy("ym")
surge = monthly \
    .withColumn("prev", lag("m_ratio").over(wm)) \
    .withColumn(
        "onset",
        when(
            (col("m_ratio") >= SURGE_LEVEL) &
            (F.coalesce(col("prev"), lit(0.0)) < SURGE_LEVEL),
            1
        ).otherwise(0)
    ) \
    .groupBy("keyword").agg(spark_sum("onset").alias("surge_count"))

# --------------------------------------------------
# 5. 버즈량 = 소스별 문서/댓글 건수 (주류화·노출량 지표)
# --------------------------------------------------
def cnt(df, name):
    return df.groupBy("keyword").agg(count("*").alias(name))

news_c = cnt(news, "news_cnt")
blog_c = cnt(blog, "blog_cnt")
yt_c   = cnt(youtube, "yt_cnt")

# --------------------------------------------------
# 6. 부정여론 비율 = 3소스 sentiment<0 비율 단순평균
# --------------------------------------------------
def src_neg(df, name):
    return df.groupBy("keyword").agg(
        count(when(col("sentiment") < 0, 1)).alias("neg"),
        count(when(col("sentiment").isNotNull(), 1)).alias("tot")
    ).withColumn(
        "neg_" + name,
        spark_round(col("neg") / greatest(col("tot"), lit(1)), 4)
    ).select("keyword", "neg_" + name)

neg = src_neg(news, "news") \
    .join(src_neg(blog, "blog"), "keyword", "left") \
    .join(src_neg(youtube, "yt"), "keyword", "left") \
    .fillna(0.0) \
    .withColumn(
        "neg_ratio",
        spark_round((col("neg_news") + col("neg_blog") + col("neg_yt")) / lit(3.0), 4)
    ).select("keyword", "neg_ratio")

# --------------------------------------------------
# 통합
# --------------------------------------------------
feat = labels.select(
        "keyword", "label", "reliable", "peak_date", "peak_ratio",
        "pre_peak_avg", "post_peak_avg", "retention_ratio", "post_months_avail"
    ) \
    .join(pre1m,  "keyword", "left") \
    .join(vol,    "keyword", "left") \
    .join(slope,  "keyword", "left") \
    .join(surge,  "keyword", "left") \
    .join(news_c, "keyword", "left") \
    .join(blog_c, "keyword", "left") \
    .join(yt_c,   "keyword", "left") \
    .join(neg,    "keyword", "left")

feat = feat \
    .fillna({
        "news_cnt": 0, "blog_cnt": 0, "yt_cnt": 0,
        "surge_count": 0, "neg_ratio": 0.0, "volatility": 0.0
    }) \
    .withColumn("total_buzz", col("news_cnt") + col("blog_cnt") + col("yt_cnt")) \
    .withColumn(
        "spread_speed",
        when(
            col("pre_1m_avg").isNotNull(),
            spark_round(lit(100.0) / greatest(col("pre_1m_avg"), lit(1.0)), 2)
        ).otherwise(lit(None).cast("double"))
    ) \
    .withColumn(
        "shortform_era",
        when(spark_year(col("peak_date")) >= 2021, lit(1)).otherwise(lit(0))
    ) \
    .withColumn(
        "label_num",
        when(col("label") == "정착", lit(1.0)).otherwise(lit(0.0))
    )

print("=== 피처 마스터 테이블 ===")
feat.select(
    "keyword", "label", "reliable",
    "spread_speed", "volatility", "decay_slope", "surge_count",
    "total_buzz", "neg_ratio", "shortform_era"
).orderBy("label", col("decay_slope")).show(40, truncate=False)

print("=== 피처 요약통계 (describe) ===")
feat.select(
    "spread_speed", "volatility", "decay_slope", "surge_count",
    "total_buzz", "neg_ratio"
).describe().show()

# 저장
print("=== fnb_features 저장 ===")
spark.sql("DROP TABLE IF EXISTS fnb_features_tmp")
feat.write.mode("overwrite").saveAsTable("fnb_features_tmp")
spark.sql("DROP TABLE IF EXISTS fnb_features")
spark.sql("ALTER TABLE fnb_features_tmp RENAME TO fnb_features")
print("fnb_features 저장 완료")

spark.stop()
print("\n피처 생성 완료!")

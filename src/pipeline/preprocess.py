# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_date, year, month, lit
from pyspark.sql.types import FloatType, StringType
import re

spark = SparkSession.builder \
    .appName("FnbTrendPreprocess") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_BASE = "hdfs:///user/maria_dev/fnb"

# 뉴스/블로그 API 응답의 HTML 태그 제거
def clean_html(text):
    if text is None:
        return ""
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

clean_html_udf = udf(clean_html, StringType())


# 1. DataLab
print("[1/4] DataLab")
datalab = spark.read.csv(f"{HDFS_BASE}/datalab/all_keywords.csv", header=True, inferSchema=True)
datalab = datalab \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .filter(col("date").isNotNull()) \
    .filter(col("ratio").isNotNull()) \
    .select("keyword", "date", "year", "month", "ratio")
print(f"DataLab: {datalab.count()}건")
datalab.write.mode("overwrite").saveAsTable("fnb_datalab")


# 2. News — year는 수집 시 pubDate 기준으로 이미 설정됨
print("[2/4] News")
news = spark.read.csv(f"{HDFS_BASE}/news/all_keywords.csv", header=True, inferSchema=True)
news = news \
    .withColumn("title_clean", clean_html_udf(col("title"))) \
    .withColumn("desc_clean", clean_html_udf(col("description"))) \
    .withColumn("pubDate", to_date(col("pubDate").cast(StringType()))) \
    .withColumn("sentiment", lit(None).cast(FloatType())) \
    .filter(col("pubDate").isNotNull()) \
    .select("keyword", "year", "title_clean", "desc_clean", "pubDate", "sentiment")
print(f"News: {news.count()}건")
news.write.mode("overwrite").saveAsTable("fnb_news")


# 3. Blog — year는 수집 시 postdate 기준으로 이미 설정됨
print("[3/4] Blog")
blog = spark.read.csv(f"{HDFS_BASE}/blog/all_keywords.csv", header=True, inferSchema=True)
blog = blog \
    .withColumn("title_clean", clean_html_udf(col("title"))) \
    .withColumn("desc_clean", clean_html_udf(col("description"))) \
    .withColumn("postdate", to_date(col("postdate").cast(StringType()))) \
    .withColumn("sentiment", lit(None).cast(FloatType())) \
    .filter(col("postdate").isNotNull()) \
    .select("keyword", "year", "title_clean", "desc_clean", "postdate", "sentiment")
print(f"Blog: {blog.count()}건")
blog.write.mode("overwrite").saveAsTable("fnb_blog")


# 4. YouTube — video_id/video_published 드롭, year는 comment_date 기준 파생
print("[4/4] YouTube")
yt = spark.read.csv(f"{HDFS_BASE}/youtube/all_keywords.csv", header=True, inferSchema=True)
yt = yt \
    .withColumn("comment_date", to_date(col("comment_date").cast(StringType()))) \
    .withColumn("year", year(col("comment_date"))) \
    .withColumn("sentiment", lit(None).cast(FloatType())) \
    .filter(col("comment_date").isNotNull()) \
    .filter(col("comment").isNotNull()) \
    .select("keyword", "video_title", "view_count",
            "comment", "comment_date", "year", "comment_likes", "sentiment")
print(f"YouTube: {yt.count()}건")
yt.write.mode("overwrite").saveAsTable("fnb_youtube")


# 완료 확인
spark.sql("SHOW TABLES").show()
spark.sql("SELECT * FROM fnb_datalab LIMIT 3").show()
spark.sql("SELECT keyword, year, pubDate, title_clean FROM fnb_news LIMIT 3").show()
spark.sql("SELECT keyword, year, postdate, title_clean FROM fnb_blog LIMIT 3").show()
spark.sql("SELECT keyword, year, comment_date, comment FROM fnb_youtube LIMIT 3").show()

spark.stop()
print("전처리 완료!")

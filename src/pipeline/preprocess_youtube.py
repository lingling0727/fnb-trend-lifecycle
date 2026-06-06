# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_date
from pyspark.sql.types import FloatType, StringType
import re

spark = SparkSession.builder \
    .appName("FnbYoutubePreprocess") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

POS_WORDS = [
    "맛있", "맛집", "추천", "최고", "좋아", "훌륭", "맛나", "꿀맛",
    "대박", "인기", "유명", "핫하", "열풍", "히트", "성공",
    "사랑", "완벽", "신선", "건강", "달콤", "고소", "향긋",
    "강추", "극찬", "감동", "행복", "최애", "레전드", "찐맛"
]
NEG_WORDS = [
    "별로", "실망", "최악", "불만", "위생", "사기", "바가지",
    "질리", "물리", "비싸", "논란", "피해", "폐업",
    "망하", "사라지", "문닫", "폭망", "퇴물",
    "한물", "식어", "시들", "잊혀", "지겨", "싫", "아쉽", "실패", "거품",
    "줄폐업", "흥행실패", "반짝유행", "유행끝"
]

def sentiment_score(text):
    if text is None or len(text) == 0:
        return 0.0
    pos = sum(1 for w in POS_WORDS if w in text)
    neg = sum(1 for w in NEG_WORDS if w in text)
    total = len(text.split())
    if total == 0:
        return 0.0
    return float(pos - neg) / max(total, 1)

sentiment_udf = udf(sentiment_score, FloatType())

print("=== Loading YouTube ===")
yt = spark.read.csv(
    "hdfs:///user/maria_dev/fnb/youtube/all_keywords.csv",
    header=True, inferSchema=True
)

yt = yt \
    .withColumn("comment_date", to_date(col("comment_date").cast(StringType()))) \
    .withColumn("sentiment", sentiment_udf(col("comment"))) \
    .filter(col("comment_date").isNotNull()) \
    .filter(col("comment").isNotNull()) \
    .select("keyword", "video_id", "video_title", "video_published",
            "view_count", "comment", "comment_date", "comment_likes", "sentiment")

print("YouTube count: {}".format(yt.count()))
yt.write.mode("overwrite").saveAsTable("fnb_youtube")
print("fnb_youtube saved")

print("\n=== Sample ===")
spark.sql("SELECT keyword, comment_date, sentiment FROM fnb_youtube LIMIT 5").show()

print("\n=== Row count per keyword (top 5) ===")
spark.sql("SELECT keyword, count(*) as cnt FROM fnb_youtube GROUP BY keyword ORDER BY cnt DESC LIMIT 5").show()

spark.stop()
print("Done!")

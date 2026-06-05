# -*- coding: utf-8 -*-
"""
전처리 파이프라인
- HDFS에서 DataLab / 뉴스 / 블로그 데이터 읽기
- 날짜 정규화, HTML 태그 제거, 텍스트 정제
- KNU 감성사전 기반 긍/부정 점수 산출
- 결과를 Hive 테이블로 저장
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, regexp_replace, to_date, year, month,
    when, lit, trim, lower
)
from pyspark.sql.types import FloatType, StringType
import re

# ── SparkSession (Hive 메타스토어 연결 포함) ──────────────────────────
# enableHiveSupport(): Spark가 Hive 테이블로 저장/조회 가능하게 함
# master("yarn"): HDP의 YARN 클러스터에서 분산 실행
spark = SparkSession.builder \
    .appName("FnbTrendPreprocess") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── HDFS 경로 ────────────────────────────────────────────────────────
HDFS_BASE = "hdfs:///user/maria_dev/fnb"

# ── 감성사전 (KNU 기반 경량 버전) ────────────────────────────────────
# 실제 KNU SentiWord Dict에서 F&B/SNS 도메인에 자주 나오는 단어만 추출
POS_WORDS = [
    "맛있", "맛집", "추천", "최고", "좋아", "훌륭", "맛나", "꿀맛",
    "성공", "대박", "인기", "유명", "핫하", "트렌드", "열풍", "히트",
    "사랑", "좋은", "완벽", "신선", "건강", "달콤", "고소", "향긋"
]
NEG_WORDS = [
    "별로", "실망", "최악", "불만", "위생", "사기", "바가지", "질려",
    "물렸", "비싸", "논란", "문제", "사망", "피해", "폐업", "망해",
    "지겨", "싫", "아쉽", "실패", "줄폐업", "거품", "과대",
    "망했", "망함", "사라졌", "없어졌", "문닫", "폭망", "흥행실패",
    "퇴물", "한물", "식었", "시들", "질림", "잊혀", "지나갔",
    "반짝", "유행끝", "벌써옛날", "오래됐"
]

# ── UDF 정의 ─────────────────────────────────────────────────────────
# UDF(User Defined Function): Spark DataFrame에 파이썬 함수를 컬럼 단위로 적용
# HTML 태그 제거 함수
def clean_html(text):
    if text is None:
        return ""
    # <b>, </b> 등 HTML 태그 제거
    text = re.sub(r'<[^>]+>', '', text)
    # 연속 공백 정리
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# 감성 점수 계산: (긍정단어 수 - 부정단어 수) / 전체 단어 수
# 결과: -1.0 ~ 1.0 (음수일수록 부정, 양수일수록 긍정)
def sentiment_score(text):
    if text is None or len(text) == 0:
        return 0.0
    pos = sum(1 for w in POS_WORDS if w in text)
    neg = sum(1 for w in NEG_WORDS if w in text)
    total = len(text.split())
    if total == 0:
        return 0.0
    return float(pos - neg) / max(total, 1)

clean_html_udf = udf(clean_html, StringType())
sentiment_udf = udf(sentiment_score, FloatType())


# ════════════════════════════════════════════════════════════════════
# 1. DataLab 전처리
# ════════════════════════════════════════════════════════════════════
print("=== Loading DataLab ===")
datalab = spark.read.csv(
    f"{HDFS_BASE}/datalab/all_keywords.csv",
    header=True, inferSchema=True
)

# date 컬럼을 날짜 타입으로 변환
datalab = datalab \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .filter(col("ratio").isNotNull()) \
    .filter(col("date").isNotNull())

print(f"DataLab count: {datalab.count()}")
datalab.printSchema()

# Hive 테이블로 저장
# saveAsTable: HDFS에 Parquet 포맷으로 저장하고 Hive 메타스토어에 등록
# → 이후 spark.sql("SELECT * FROM fnb_datalab")로 조회 가능
datalab.write.mode("overwrite").saveAsTable("fnb_datalab")
print("fnb_datalab saved")


# ════════════════════════════════════════════════════════════════════
# 2. News
# ════════════════════════════════════════════════════════════════════
print("=== Loading News ===")
news = spark.read.csv(
    f"{HDFS_BASE}/news/all_keywords.csv",
    header=True, inferSchema=True
)

news = news \
    .withColumn("title_clean", clean_html_udf(col("title"))) \
    .withColumn("desc_clean", clean_html_udf(col("description"))) \
    .withColumn("text", col("title_clean").cast(StringType())) \
    .withColumn("sentiment", sentiment_udf(col("text"))) \
    .withColumn("pubDate", to_date(col("pubDate").cast(StringType()))) \
    .filter(col("pubDate").isNotNull()) \
    .select("keyword", "year", "title_clean", "desc_clean",
            "pubDate", "sentiment")

print(f"News count: {news.count()}")
news.write.mode("overwrite").saveAsTable("fnb_news")
print("fnb_news saved")


# ════════════════════════════════════════════════════════════════════
# 3. Blog
# ════════════════════════════════════════════════════════════════════
print("=== Loading Blog ===")
blog = spark.read.csv(
    f"{HDFS_BASE}/blog/all_keywords.csv",
    header=True, inferSchema=True
)

blog = blog \
    .withColumn("title_clean", clean_html_udf(col("title"))) \
    .withColumn("desc_clean", clean_html_udf(col("description"))) \
    .withColumn("text", col("title_clean").cast(StringType())) \
    .withColumn("sentiment", sentiment_udf(col("text"))) \
    .withColumn("postdate", to_date(col("postdate").cast(StringType()))) \
    .filter(col("postdate").isNotNull()) \
    .select("keyword", "year", "title_clean", "desc_clean",
            "postdate", "sentiment")

print(f"Blog count: {blog.count()}")
blog.write.mode("overwrite").saveAsTable("fnb_blog")
print("fnb_blog saved")


# ════════════════════════════════════════════════════════════════════
# 4. Done
# ════════════════════════════════════════════════════════════════════
print("\n=== Tables ===")
spark.sql("SHOW TABLES").show()

print("\n=== DataLab sample ===")
spark.sql("SELECT * FROM fnb_datalab LIMIT 5").show()

print("\n=== News sentiment sample ===")
spark.sql("""
    SELECT keyword, pubDate, sentiment
    FROM fnb_news
    ORDER BY pubDate DESC
    LIMIT 5
""").show()

spark.stop()
print("\nPreprocessing done!")

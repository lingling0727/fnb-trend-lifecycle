# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, concat_ws, explode, split, lower, trim
from pyspark.sql.types import FloatType, BooleanType

spark = SparkSession.builder \
    .appName("FnbSentimentApply") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# KNU SentiWord_Dict 로드 — 박상민 외, 경북대학교 (2018)
# 형식: 단어\t점수 (-2 ~ 2)
DICT_PATH = "/home/maria_dev/SentiWord_Dict.txt"

senti_dict = {}
with open(DICT_PATH, "r", encoding="utf-8") as f:
    for line in f:
        parts = line.strip().split("\t")
        if len(parts) == 2:
            try:
                senti_dict[parts[0]] = int(parts[1])
            except ValueError:
                continue

print(f"KNU 사전 로드: {len(senti_dict)}개 단어")

# broadcast — 드라이버에서 읽어 모든 executor에 배포
bc_dict = spark.sparkContext.broadcast(senti_dict)


# 감성 점수 UDF
# 공식: sum(매칭 점수) / (매칭 수 × 2) → 범위 -1.0 ~ 1.0
# 참조: Taboada et al. (2011) SO-CAL
def make_sentiment_udf(bc):
    def sentiment_score(text):
        if not text:
            return 0.0
        d = bc.value
        matched = [score for word, score in d.items() if word in text]
        if not matched:
            return 0.0
        return float(sum(matched)) / (len(matched) * 2)
    return udf(sentiment_score, FloatType())

sentiment_udf = make_sentiment_udf(bc_dict)


# 임시 테이블에 먼저 쓴 뒤 rename — 같은 테이블 읽기/쓰기 충돌 방지
def overwrite_table(df, table_name):
    tmp = table_name + "_tmp"
    spark.sql(f"DROP TABLE IF EXISTS {tmp}")
    df.write.mode("overwrite").saveAsTable(tmp)
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"ALTER TABLE {tmp} RENAME TO {table_name}")

# 1. News — title_clean + desc_clean 합산
print("[1/3] News")
news = spark.table("fnb_news") \
    .withColumn("sentiment", sentiment_udf(concat_ws(" ", col("title_clean"), col("desc_clean"))))
overwrite_table(news, "fnb_news")
print("fnb_news 완료")

# 2. Blog — title_clean + desc_clean 합산
print("[2/3] Blog")
blog = spark.table("fnb_blog") \
    .withColumn("sentiment", sentiment_udf(concat_ws(" ", col("title_clean"), col("desc_clean"))))
overwrite_table(blog, "fnb_blog")
print("fnb_blog 완료")

# 3. YouTube — 댓글 전체
print("[3/3] YouTube")
yt = spark.table("fnb_youtube") \
    .withColumn("sentiment", sentiment_udf(col("comment")))
overwrite_table(yt, "fnb_youtube")
print("fnb_youtube 완료")


# 소스별 감성 점수 분포 확인
spark.sql("""
    SELECT 'news' AS src, COUNT(*) AS total,
           ROUND(AVG(sentiment), 4) AS avg_sent,
           ROUND(MIN(sentiment), 4) AS min_sent,
           ROUND(MAX(sentiment), 4) AS max_sent
    FROM fnb_news
    UNION ALL
    SELECT 'blog', COUNT(*), ROUND(AVG(sentiment),4), ROUND(MIN(sentiment),4), ROUND(MAX(sentiment),4)
    FROM fnb_blog
    UNION ALL
    SELECT 'youtube', COUNT(*), ROUND(AVG(sentiment),4), ROUND(MIN(sentiment),4), ROUND(MAX(sentiment),4)
    FROM fnb_youtube
""").show()

# 키워드별 평균 감성 (뉴스 기준, 낮은 순)
spark.sql("""
    SELECT keyword, COUNT(*) AS cnt, ROUND(AVG(sentiment), 4) AS avg_sentiment
    FROM fnb_news
    GROUP BY keyword
    ORDER BY avg_sentiment ASC
    LIMIT 10
""").show()


# [C옵션] 블로그 고빈도 단어 중 KNU 미등록 상위 30개
# → 수동 추가 후보 확인용
print("[C] 블로그 고빈도 단어 중 KNU 미등록 상위 30개")

bc_knu_keys = spark.sparkContext.broadcast(set(senti_dict.keys()))

def not_in_knu(word):
    return word not in bc_knu_keys.value

not_in_knu_udf = udf(not_in_knu, BooleanType())

spark.table("fnb_blog") \
    .select(explode(split(trim(lower(
        concat_ws(" ", col("title_clean"), col("desc_clean"))
    )), " ")).alias("word")) \
    .filter(col("word").rlike("[가-힣]{2,}")) \
    .groupBy("word").count() \
    .filter(not_in_knu_udf(col("word"))) \
    .orderBy("count", ascending=False) \
    .limit(30) \
    .show(30, truncate=False)

spark.stop()
print("감성 분석 완료!")

# -*- coding: utf-8 -*-
# fnb_features 기반: Q4(버즈량) + 통계검정(Cohen's d / t) + MLlib 피처 중요도
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, corr, round as spark_round
import pyspark.sql.functions as F

from pyspark.ml.feature import Imputer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

spark = SparkSession.builder \
    .appName("FnbML") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/apps/spark/warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

feat = spark.table("fnb_features")
n_total = feat.count()
print(f"=== 분석 대상 키워드 수: {n_total} ===")

# ==================================================
# Q4: 버즈량(미디어 노출량)이 정착을 설명하는가?
# ==================================================
print("\n" + "="*60)
print("Q4: 버즈량 vs 정착/지속성")
print("="*60)

feat_q4 = feat.withColumn("log_buzz", F.log1p(col("total_buzz")))

print("[Q4 상관계수]")
feat_q4.agg(
    spark_round(corr("total_buzz", "post_peak_avg"), 4).alias("buzz_vs_지속성"),
    spark_round(corr("total_buzz", "label_num"),     4).alias("buzz_vs_정착여부"),
    spark_round(corr("log_buzz",   "post_peak_avg"), 4).alias("logbuzz_vs_지속성")
).show()

print("[Q4 정착/소멸 그룹별 평균 버즈량]")
feat.groupBy("label").agg(
    F.round(avg("news_cnt"), 0).alias("avg_news"),
    F.round(avg("blog_cnt"), 0).alias("avg_blog"),
    F.round(avg("yt_cnt"),   0).alias("avg_yt"),
    F.round(avg("total_buzz"), 0).alias("avg_total"),
    count("*").alias("n")
).show()

# ==================================================
# 통계검정: 정착 vs 소멸 그룹 차이 (Cohen's d + t)
# scipy 없이 순수 계산. |t| > 2.03 이면 df≈36 기준 p<0.05 근사.
# ==================================================
print("\n" + "="*60)
print("그룹 차이 통계검정 (정착 vs 소멸)")
print("Cohen's d: 0.2 소 / 0.5 중 / 0.8 대   |   |t|>~2.03 ≈ p<0.05")
print("="*60)

stat_features = [
    "spread_speed", "pre_peak_avg", "post_peak_avg", "volatility",
    "decay_slope", "surge_count", "total_buzz", "neg_ratio"
]

exprs = []
for f in stat_features:
    exprs += [
        avg(f).alias("m_" + f),
        F.stddev(f).alias("s_" + f),
        count(f).alias("n_" + f),
    ]
rows = {r["label"]: r for r in feat.groupBy("label").agg(*exprs).collect()}

A = rows.get("정착")
B = rows.get("소멸")

print("\n{:<16}{:>10}{:>10}{:>10}{:>10}".format("feature", "정착평균", "소멸평균", "Cohen_d", "t_stat"))
print("-" * 56)
if A is not None and B is not None:
    for f in stat_features:
        m1, s1, n1 = A["m_" + f], A["s_" + f], A["n_" + f]
        m2, s2, n2 = B["m_" + f], B["s_" + f], B["n_" + f]
        if None in (m1, m2, s1, s2) or n1 < 2 or n2 < 2:
            continue
        pooled_var = ((n1 - 1) * s1 * s1 + (n2 - 1) * s2 * s2) / (n1 + n2 - 2)
        pooled_sd = math.sqrt(pooled_var) if pooled_var > 0 else 0.0
        if pooled_sd == 0:
            d_eff = t_stat = 0.0
        else:
            d_eff = (m1 - m2) / pooled_sd
            t_stat = (m1 - m2) / (pooled_sd * math.sqrt(1.0 / n1 + 1.0 / n2))
        sig = " *" if abs(t_stat) > 2.03 else ""
        print("{:<16}{:>10.2f}{:>10.2f}{:>10.2f}{:>10.2f}{}".format(
            f, m1, m2, d_eff, t_stat, sig))
print("\n(* = |t|>2.03, df≈36 기준 통계적으로 유의 추정)")

# ==================================================
# MLlib: 정착/소멸 분류 피처 중요도
# 누수(leakage) 방지: 라벨을 직접 정의하는 post_peak_avg / retention / decay_slope 제외
#   → '피크 전 + 전 기간 특성'만으로 정착을 설명
# ==================================================
print("\n" + "="*60)
print("MLlib 피처 중요도 (정착/소멸 분류)")
print("주의: n=38 → 예측 모델이 아니라 '변수 중요도 해석'용. 과적합 전제.")
print("="*60)

ml_cols = [
    "spread_speed", "pre_peak_avg", "volatility",
    "surge_count", "total_buzz", "neg_ratio", "shortform_era"
]

# spread_speed NULL → 중앙값 대치 (Spark 2.3 VectorAssembler는 NULL 불가)
imputer = Imputer(strategy="median",
                  inputCols=["spread_speed"],
                  outputCols=["spread_speed_i"])
fm = imputer.fit(feat).transform(feat)

assemble_cols = ["spread_speed_i", "pre_peak_avg", "volatility",
                 "surge_count", "total_buzz", "neg_ratio", "shortform_era"]
for c in assemble_cols:
    fm = fm.withColumn(c, col(c).cast("double"))

assembler = VectorAssembler(inputCols=assemble_cols, outputCol="features")
data = assembler.transform(fm).select("keyword", "label_num", "features").cache()

# --- RandomForest 피처 중요도 ---
rf = RandomForestClassifier(labelCol="label_num", featuresCol="features",
                            numTrees=200, maxDepth=4, seed=42)
rf_model = rf.fit(data)

print("\n[RandomForest featureImportances]")
imp = sorted(zip(ml_cols, rf_model.featureImportances.toArray()),
             key=lambda x: -x[1])
for name, score in imp:
    bar = "#" * int(round(score * 50))
    print("  {:<14}{:>7.4f}  {}".format(name, score, bar))

# --- 정확도: 학습 정확도 + 5-fold CV ---
evaluator = MulticlassClassificationEvaluator(
    labelCol="label_num", predictionCol="prediction", metricName="accuracy")
train_acc = evaluator.evaluate(rf_model.transform(data))

cv = CrossValidator(
    estimator=rf,
    estimatorParamMaps=ParamGridBuilder().build(),
    evaluator=evaluator,
    numFolds=5, seed=42)
cv_model = cv.fit(data)
cv_acc = cv_model.avgMetrics[0]

base_rate = data.filter(col("label_num") == 0).count() / float(n_total)  # 다수클래스(소멸) 비율
print("\n[정확도]")
print("  학습 정확도(과적합):   {:.3f}".format(train_acc))
print("  5-fold CV 정확도:      {:.3f}".format(cv_acc))
print("  기준선(전부 소멸 찍기): {:.3f}".format(base_rate))

# --- LogisticRegression 표준화 계수 (방향성) ---
scaler = StandardScaler(inputCol="features", outputCol="sfeatures",
                        withMean=True, withStd=True)
sdata = scaler.fit(data).transform(data)
lr = LogisticRegression(labelCol="label_num", featuresCol="sfeatures", maxIter=200)
lr_model = lr.fit(sdata)

print("\n[LogisticRegression 표준화 계수 (+면 정착, -면 소멸 방향)]")
coefs = sorted(zip(ml_cols, lr_model.coefficients.toArray()),
               key=lambda x: -abs(x[1]))
for name, c in coefs:
    print("  {:<14}{:>+8.4f}".format(name, c))

data.unpersist()
spark.stop()
print("\nML/통계 분석 완료!")

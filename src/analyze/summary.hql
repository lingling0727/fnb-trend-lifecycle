-- F&B 트렌드 생애주기 분석 결과 요약
-- 실행: spark-sql --conf spark.sql.warehouse.dir=/apps/spark/warehouse -f summary.hql

-- ============================================================
-- 1. 정착/소멸 라벨 분포
-- ============================================================
SELECT '=== 라벨 분포 ===' AS section;

SELECT
    label,
    COUNT(*)                          AS n,
    ROUND(AVG(retention_ratio), 4)   AS avg_retention,
    ROUND(AVG(pre_peak_avg),  2)     AS avg_pre_peak,
    ROUND(AVG(post_peak_avg), 2)     AS avg_post_peak
FROM fnb_labels
GROUP BY label
ORDER BY label;

-- ============================================================
-- 2. 신뢰 키워드(reliable=true)만 필터링한 라벨 분포
-- ============================================================
SELECT '=== 신뢰 키워드 라벨 분포 ===' AS section;

SELECT
    label,
    COUNT(*) AS n
FROM fnb_labels
WHERE reliable = true
GROUP BY label
ORDER BY label;

-- ============================================================
-- 3. 정착/소멸 그룹별 피처 평균 비교
-- ============================================================
SELECT '=== 그룹별 피처 평균 ===' AS section;

SELECT
    label,
    ROUND(AVG(spread_speed),  2) AS avg_spread_speed,
    ROUND(AVG(volatility),    2) AS avg_volatility,
    ROUND(AVG(surge_count),   2) AS avg_surge_count,
    ROUND(AVG(total_buzz),    0) AS avg_total_buzz,
    ROUND(AVG(neg_ratio),     4) AS avg_neg_ratio,
    COUNT(*)                     AS n
FROM fnb_features
GROUP BY label
ORDER BY label;

-- ============================================================
-- 4. 숏폼 이전/이후 × 정착/소멸 교차표
-- ============================================================
SELECT '=== 숏폼 전후 × 정착/소멸 (신뢰 키워드) ===' AS section;

SELECT
    CASE WHEN shortform_era = 1 THEN '숏폼 이후(2021~)' ELSE '숏폼 이전(~2020)' END AS era,
    label,
    COUNT(*) AS n
FROM fnb_features
WHERE keyword IN (SELECT keyword FROM fnb_labels WHERE reliable = true)
GROUP BY shortform_era, label
ORDER BY shortform_era, label;

-- ============================================================
-- 5. 키워드별 요약 (정착 상위 / 소멸 상위 retention 기준)
-- ============================================================
SELECT '=== 정착 키워드 (retention 상위 5) ===' AS section;

SELECT
    keyword,
    peak_date,
    ROUND(retention_ratio, 4) AS retention_ratio,
    ROUND(post_peak_avg,   2) AS post_peak_avg
FROM fnb_labels
WHERE label = '정착'
ORDER BY retention_ratio DESC
LIMIT 5;

SELECT '=== 소멸 키워드 (retention 하위 5) ===' AS section;

SELECT
    keyword,
    peak_date,
    ROUND(retention_ratio, 4) AS retention_ratio,
    ROUND(post_peak_avg,   2) AS post_peak_avg
FROM fnb_labels
WHERE label = '소멸'
ORDER BY retention_ratio ASC
LIMIT 5;

#!/bin/bash
# F&B 트렌드 생애주기 분석 — 전체 파이프라인 자동화
#
# 사전 조건:
#   1. GCP VM 실행 중 + SSH 터널 연결 상태
#      gcloud compute ssh <instance> -- -L 2222:localhost:2222 -N &
#   2. HDP Docker 컨테이너 실행 중
#
# API 키 유무에 따라 자동 분기:
#   - .env에 API 키 있음 → 실제 수집 후 전체 파이프라인 실행
#   - .env 없거나 키 누락  → data/sample/ 사용 (수집 생략)
#
# 사용법: bash run_pipeline.sh

set -e

# --------------------------------------------------
# 설정
# --------------------------------------------------
HDP_USER="maria_dev"
HDP_HOST="localhost"
HDP_PORT="2222"
REMOTE_DIR="/home/maria_dev"
HDFS_DIR="/user/maria_dev/fnb"
SPARK="PYSPARK_PYTHON=python3.6 PYTHONIOENCODING=utf-8 spark-submit --master yarn --deploy-mode client"
SPARKSQL="JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8 spark-sql --conf spark.sql.warehouse.dir=/apps/spark/warehouse"
LOG_DIR="logs"
ENV_FILE=".env"

mkdir -p "$LOG_DIR"

echo "=============================================="
echo " F&B 트렌드 생애주기 분석 파이프라인"
echo "=============================================="

# --------------------------------------------------
# 0. HDP SSH 연결 확인
# --------------------------------------------------
echo ""
echo "[0] HDP 연결 확인..."

if ! ssh -p "$HDP_PORT" -o ConnectTimeout=5 -o BatchMode=yes \
        "${HDP_USER}@${HDP_HOST}" "echo ok" &>/dev/null; then
    echo "오류: HDP SSH 연결 실패 (port ${HDP_PORT})"
    echo "  → gcloud compute ssh <instance> -- -L 2222:localhost:2222 -N &"
    exit 1
fi
echo "HDP 연결 확인 완료"

# --------------------------------------------------
# 1. API 키 확인 → 수집 or 샘플 분기
# --------------------------------------------------
echo ""

USE_SAMPLE=true

if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' "$ENV_FILE" | xargs 2>/dev/null)
    if [ -n "$NAVER_CLIENT_ID" ] && [ -n "$NAVER_CLIENT_SECRET" ] && [ -n "$YOUTUBE_API_KEY" ]; then
        USE_SAMPLE=false
    fi
fi

if [ "$USE_SAMPLE" = false ]; then
    echo "[1] API 키 감지 → 실제 데이터 수집 시작..."

    python3 src/ingest/naver_datalab.py 2>"${LOG_DIR}/ingest_datalab.log"
    echo "  DataLab 수집 완료"

    python3 src/ingest/naver_news.py 2>"${LOG_DIR}/ingest_news.log"
    echo "  뉴스 수집 완료"

    python3 src/ingest/naver_blog.py 2>"${LOG_DIR}/ingest_blog.log"
    echo "  블로그 수집 완료"

    python3 src/ingest/youtube.py 2>"${LOG_DIR}/ingest_youtube.log"
    echo "  유튜브 수집 완료"

    DATA_DIR="data/raw"
else
    echo "[1] API 키 없음 → 샘플 데이터 사용 (data/sample/)"
    DATA_DIR="data/sample"
fi

# --------------------------------------------------
# 2. 데이터 HDFS 업로드
# --------------------------------------------------
echo ""
echo "[2] HDFS 업로드 (${DATA_DIR})..."

scp -P "$HDP_PORT" -r "${DATA_DIR}/" "${HDP_USER}@${HDP_HOST}:${REMOTE_DIR}/fnb_data/"

ssh -p "$HDP_PORT" "${HDP_USER}@${HDP_HOST}" bash <<EOF
    hdfs dfs -mkdir -p ${HDFS_DIR}
    hdfs dfs -put -f ${REMOTE_DIR}/fnb_data/ ${HDFS_DIR}/
    echo "HDFS 적재 완료"
    hdfs dfs -du -h ${HDFS_DIR}
EOF

echo "HDFS 업로드 완료"

# --------------------------------------------------
# 3. 스크립트 업로드
# --------------------------------------------------
echo ""
echo "[3] 분석 스크립트 업로드..."

scp -P "$HDP_PORT" \
    src/pipeline/preprocess.py \
    src/pipeline/apply_sentiment.py \
    src/pipeline/label.py \
    src/analyze/analyze.py \
    src/analyze/lifecycle.py \
    src/analyze/features.py \
    src/analyze/ml.py \
    src/analyze/summary.hql \
    "${HDP_USER}@${HDP_HOST}:${REMOTE_DIR}/"

# 감성사전 업로드 (없으면)
ssh -p "$HDP_PORT" "${HDP_USER}@${HDP_HOST}" \
    "[ -f ${REMOTE_DIR}/SentiWord_Dict.txt ] && echo '  사전 파일 이미 존재'" \
    || scp -P "$HDP_PORT" SentiWord_Dict.txt "${HDP_USER}@${HDP_HOST}:${REMOTE_DIR}/"

echo "스크립트 업로드 완료"

# --------------------------------------------------
# 4. Spark 파이프라인
# --------------------------------------------------
echo ""
echo "[4] Spark 파이프라인 실행..."

SPARK_SCRIPTS=(
    "preprocess.py"
    "apply_sentiment.py"
    "label.py"
    "analyze.py"
    "lifecycle.py"
    "features.py"
    "ml.py"
)

for script in "${SPARK_SCRIPTS[@]}"; do
    echo "  실행 중: $script"
    ssh -p "$HDP_PORT" "${HDP_USER}@${HDP_HOST}" \
        "cd ${REMOTE_DIR} && ${SPARK} ${script}" \
        2>"${LOG_DIR}/${script%.py}.log"
    echo "  완료: $script"
done

# --------------------------------------------------
# 5. HiveQL 요약
# --------------------------------------------------
echo ""
echo "[5] HiveQL 요약 쿼리 실행..."

ssh -p "$HDP_PORT" "${HDP_USER}@${HDP_HOST}" \
    "cd ${REMOTE_DIR} && ${SPARKSQL} -f summary.hql" \
    2>"${LOG_DIR}/summary_hive.log" \
    | tee "${LOG_DIR}/summary_result.txt"

echo "HiveQL 완료 → logs/summary_result.txt"

# --------------------------------------------------
# 완료
# --------------------------------------------------
echo ""
echo "=============================================="
if [ "$USE_SAMPLE" = true ]; then
    echo " 완료 [샘플 데이터 모드]"
else
    echo " 완료 [실제 수집 모드]"
fi
echo " 로그: ${LOG_DIR}/"
echo " 결과: ${LOG_DIR}/summary_result.txt"
echo "=============================================="

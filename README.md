# fnb-trend-lifecycle
## F&B 트렌드 생애주기 빅데이터 분석 파이프라인

> 왜 어떤 F&B 유행은 문화로 정착하고(불닭), 어떤 유행은 휘발되어 사라지는가(탕후루)?
> 이 차이를 데이터로 증명하는 것이 목표다.


## 1. 문제 정의

최근 F&B 트렌드는 숏폼 등 K-콘텐츠를 타고 빠르게 확산되지만, 서로 다른 생애주기를 보인다.

- 문화 정착형 (예: 불닭볶음면) — 피크 이후에도 검색량이 일정 수준 유지
- 휘발 소멸형 (예: 탕후루) — 피크 후 급락, 회복되지 않음

38개 F&B 키워드의 검색량·뉴스·블로그·유튜브 데이터를 수집·통합하여, 두 결말을 가르는 데이터 패턴을 분산 처리 파이프라인으로 분석한다.

### 키워드 선정

트렌드 리포트(배달의민족·aT 등 공식 리포트) 및 온라인 미디어 모니터링(유튜브·커뮤니티·백과사전류)을 통해 후보 키워드를 구성했다. 이후 네이버 DataLab 검색량 피크 기준 OR 공식 리포트 등장 여부로 최종 38개를 선정했다. 분석 기준 시점은 2016년(네이버 DataLab 서비스 시작)으로 통일했다.

### 분석 질문

- Q1. 유행의 초기 확산 속도와 지속성 사이에 패턴이 있는가?
- Q2. 부정 여론 비율이 높을수록 쇠퇴 속도가 빠른가?
- Q3. 숏폼 대중화 이후(2021~) 트렌드의 생애주기가 달라졌는가?
- Q4. 미디어 노출량(언급 문서 수)이 많을수록 정착 가능성이 높은가?

### 주요 지표 정의

DataLab은 키워드별로 최고치를 100으로 정규화하므로, 키워드 간 절대 비교 대신 키워드-내부 비율 지표를 사용한다.

- 확산속도(spread_speed) = 100 ÷ 피크 직전 1개월 평균 — 값이 클수록 무명에서 갑자기 폭발(급발진), 작을수록 점진적 형성
- 사전 인지도(pre_peak_avg) = 피크 이전 12개월 평균 검색량
- 지속성(post_peak_avg) = 피크 이후 12개월 평균 검색량
- 지속률(retention_ratio) = 지속성 ÷ 사전 인지도 — 피크 전 대비 검색량 유지 정도
- 정착/소멸 라벨 = 지속률 ≥ 0.5 그리고 지속성 ≥ 10 이면 정착, 아니면 소멸


## 2. 분석 설계 원칙

- 데이터 — 네이버 DataLab + 뉴스 + 블로그 + 유튜브 댓글 4개 이질 소스를 수집·통합(약 38.4만 행 / 110.4MB)하고, KNU 한국어 감성사전을 SO-CAL 공식으로 한국어 F&B 텍스트에 적용해 감성 점수를 산출했다.
- 문제 — "트렌드 분석"을 일반화하지 않고 "생애주기 결말을 가르는 변수"로 재정의했으며, 정착/소멸 라벨을 사전 정의 임계값으로 자동 부여했다.
- 분석 — 아래와 같은 설계로 단순 상관 나열을 넘어섰다.
  - DataLab 키워드별 독립 정규화의 함정을 인지하고, 정규화에 불변인 키워드-내부 비율 지표만 사용 (절대 검색량 비교 회피)
  - 공정한 예측: 정착/소멸 결과가 이미 반영된 사후 지표(post_peak_avg, retention_ratio 등)와 피크 이후 정보가 섞인 지표(volatility, surge_count 등)는 ML 피처에서 제외하고, 피크 이전에 관측 가능한 지표만 사용
  - 수렴타당도 검증: 서로 다른 두 지표(spread_speed ↔ rise_days)가 같은 결론을 가리키는지 교차 확인
  - 아웃라이어 강건성: Pearson 대신 순위기반 Spearman 병기 (특정 데이터 임의 제거 회피)
  - 피크 탐지 강건성: 일별 최댓값은 DataLab 단발 글리치에 취약 → 31일 이동평균 후 '첫 주요 피크'(상승의 끝)로 정의 (탕후루 2019 글리치 vs 2023 실제 바이럴 구분)
  - 최신성 교란 통제: 피크 후 12개월 데이터 확보 여부(reliable 플래그)로 재집계
  - 효과크기(Cohen's d) + t검정 + 5-fold CV + 기준선 병기


## 3. 핵심 결과

- Q1 확산속도 — 정착 6.46 vs 소멸 38.20 (Cohen's d=-1.54, t=-4.47, 유의), 확산속도↔지속성 상관 -0.61 → 천천히 형성될수록 정착, 급발진할수록 소멸
- Q1 검증(생애주기) — 피크 도달 중앙값 정착 139일 vs 소멸 25일. 독립 지표가 같은 결론 → 수렴타당도 확보
- Q2 부정여론 — 정착 0.28 vs 소멸 0.32 (d=-0.66, t=-2.00), 부정여론↔쇠퇴속도 상관 0.50(신뢰 키워드) → 약한 보조 신호는 있으나 경계선(표본 한계로 단정 보류)
- Q3 숏폼 전후 — 신뢰 키워드 기준 숏폼 이전 정착률 47% vs 이후 60%. recency 교란(최신 키워드는 아직 소멸할 시간이 없음)을 통제해도 차이는 유지되나 인과관계는 불명확 → 판단 보류
- Q4 노출량 — 정착 7,087 vs 소멸 7,298 (d=-0.08, 유의하지 않음) → 많이 회자된다고 살아남지 않음
- rise vs fall — Spearman = 0.02 → 확산 속도와 소멸 속도는 독립 ("빨리 뜨면 빨리 죽는다" 통념 기각)
- MLlib 피처중요도 — pre_peak_avg 0.64 / spread_speed 0.34 / shortform_era 0.02 (CV 0.867 vs 기준선 0.579, 피크 이전 사전 지표만 사용)

종합: 트렌드의 생애주기를 가르는 것은 노출량이 아니라 유행이 형성되는 속도(점진 vs 급발진)와 피크 직전까지 쌓인 사전 인지도였다. 부정여론은 약한 보조 신호에 그쳤다.

### 분석의 한계

- 표본 n=38 — MLlib는 예측 모델이 아니라 변수 중요도 해석용이며 과적합 전제
- ML 피처는 피크 이전에 관측 가능한 사전 지표(spread_speed, pre_peak_avg, shortform_era)만 사용. volatility·surge_count·total_buzz·neg_ratio는 전 기간 집계라 피크 이후 정보가 섞여 제외
- 라벨은 '검색 지속성'을 측정 → 문화적 정착과 다를 수 있음. 마라탕·로제떡볶이처럼 일상 음식으로 자리잡았으나 검색 관심은 식은 경우는 '소멸'로 분류됨(정착할수록 덜 검색하는 특성)
- 라벨 임계값(retention≥0.5, post≥10)은 자의적 — 컷오프에 따라 일부 경계 키워드 라벨이 바뀔 수 있음
- 노출량은 키워드 나이에 교란 — 오래된 트렌드일수록 누적 문서가 많음


## 4. 기술 스택

| 단계 | 도구 |
|------|------|
| 데이터 수집 | Python (네이버 DataLab API, 네이버 검색 API, YouTube Data API, BeautifulSoup) |
| 저장 | HDFS (HDP 3.0.1 Sandbox) |
| 전처리·분석 | Apache Spark 2.3 (DataFrame / Spark SQL) on YARN |
| 테이블 관리·요약 쿼리 | Hive (메타스토어 연동 + HiveQL 집계 쿼리 — `summary.hql`) |
| 머신러닝 | Spark MLlib (RandomForest, LogisticRegression, CrossValidator) |
| 감성분석 | KNU 한국어 감성사전 + SO-CAL 공식 |
| 시각화 | Matplotlib |


## 5. 데이터 소스

총 약 38.4만 행 / 110.4MB, 4개 이질 소스 (정형 시계열 1 + 비정형 텍스트 3). 분석 기준 시점은 2016년 이후.

| 소스 | 유형 | 행수 | 용량 | 수집 방법 |
|------|------|-----:|-----:|----------|
| 네이버 DataLab | 정형 시계열 | 111,274 | 3.4MB | 검색어트렌드 API |
| 블로그 | 비정형 텍스트 | 176,263 | 75.8MB | 네이버 검색 API |
| 유튜브 댓글 | 비정형 텍스트 | 67,471 | 15.9MB | YouTube Data API |
| 뉴스 | 비정형 텍스트 | 28,959 | 15.3MB | 네이버 검색 API + 스크래핑 |
| 합계 | | 약 38.4만 | 110.4MB | |


## 6. 파이프라인 자동화

수집부터 결과 산출까지 `run_pipeline.sh` 한 번으로 실행된다.

```bash
bash run_pipeline.sh
```

API 키 유무에 따라 자동 분기:
- `.env`에 API 키 있음 → 실제 데이터 수집 후 전체 파이프라인 실행
- API 키 없음 → `data/sample/` 샘플 데이터로 자동 대체

실행 환경: 로컬에서 실행, GCP VM → SSH 터널 → Docker HDP Sandbox (Spark 2.3 / YARN)에 원격 제어

```
[수집]  API 키 있음: 네이버 DataLab / 뉴스 / 블로그 / 유튜브 실제 수집
        API 키 없음: data/sample/ 샘플 데이터 사용 (소스별 300행)
                              │
[저장]  HDFS 적재 (scp → hdfs dfs -put)
                              │
[전처리] Spark — 결측 처리, 타입 변환, year 파생, 텍스트 정제
                              │
[감성]  KNU 감성사전(14,851단어) × SO-CAL 공식 → sentiment 점수 (-1.0~1.0)
                              │
[피크]  31일 이동평균 후 '첫 주요 피크'(평활 최댓값의 80% 첫 도달) — 단발 이상치 방어
                              │
[라벨링] retention_ratio = post피크평균 / pre피크평균
        retention ≥ 0.5 AND post평균 ≥ 10 → 정착, else 소멸
        + reliable 플래그 (피크 후 12개월·전 1개월 데이터 확보 여부)
                              │
[분석]  Q1 확산속도 / Q2 부정여론 / Q3 숏폼 / Q4 노출량
        + 생애주기(rise/fall) + 피처 마스터테이블 + MLlib 피처중요도
                              │
[HiveQL] 그룹별 피처 평균 / 숏폼 전후 교차표 / 키워드 요약 (summary.hql)
                              │
결과: logs/summary_result.txt
```


## 7. Repository 구조

```
fnb-trend-lifecycle/
├── README.md
├── run_pipeline.sh            # 수집~산출 전체 파이프라인 자동화 (bash)
├── keywords.json              # 분석 대상 키워드 38개 (숏폼 구분은 peak_date에서 파생)
├── SentiWord_Dict.txt         # KNU 한국어 감성사전
├── data/
│   ├── raw/                   # 로컬 수집 원본 (gitignore)
│   └── sample/                # 소스별 샘플 300행 (datalab/news/blog/youtube)
├── src/
│   ├── ingest/                # 수집 — naver_datalab / naver_news / naver_blog / youtube
│   ├── pipeline/
│   │   ├── preprocess.py      # Spark 전처리 (4소스 통합)
│   │   ├── apply_sentiment.py # KNU 사전 × SO-CAL 감성 점수
│   │   └── label.py           # 정착/소멸 자동 라벨링 + reliable 플래그
│   └── analyze/
│       ├── analyze.py         # Q1·Q2·Q3 (확산속도/부정여론/숏폼)
│       ├── lifecycle.py       # 생애주기 rise/fall + Spearman 상관
│       ├── features.py        # 피처 마스터테이블(fnb_features) 생성
│       ├── ml.py              # Q4 + 통계검정 + MLlib 피처중요도
│       ├── summary.hql        # HiveQL 그룹 요약 쿼리
│       ├── export_results.py  # fnb_features → HDFS CSV 추출
│       └── visualize.py       # 분석 결과 시각화 (로컬 실행)
└── docs/
    ├── figures/               # 분석 결과 시각화 (01~06 png)
    ├── report.pdf             # 최종 보고서
    └── references/            # 참고 자료
```


## 8. 참고 자료 (출처)

- KNU 한국어 감성사전 — 군산대학교 데이터지능연구실, KnuSentiLex (https://github.com/park1200656/KnuSentiLex), 14,851개 단어, 점수 -2~+2
- SO-CAL 공식 — Taboada, M. et al. (2011). Lexicon-Based Methods for Sentiment Analysis. Computational Linguistics, 37(2). `sum(매칭점수) / (매칭수 × 2)`
- 네이버 DataLab 검색어트렌드 API — https://developers.naver.com/docs/serviceapi/datalab/search/search.md
- 네이버 검색 API (뉴스/블로그) — https://developers.naver.com/docs/serviceapi/search/
- YouTube Data API v3 — https://developers.google.com/youtube/v3


## 9. AI 도구 사용 내역

- Claude — 데이터 수집 스크립트 초안 작성, Spark 2.3 호환 코드 디버깅 (Window 함수 대체, 타입 캐스팅), 시각화 도움 (차트 구성, 색상 팔레트 제안)

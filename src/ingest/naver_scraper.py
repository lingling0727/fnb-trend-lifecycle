"""
Naver 뉴스/블로그 날짜 범위 스크래핑

[왜 API 대신 스크래핑을 쓰냐]
Naver 검색 API는 날짜 범위 파라미터를 지원하지 않아서
키워드 피크 시점 데이터를 정확히 수집하기 어려움.
웹 검색 URL에는 날짜 필터가 있어서 이를 활용함.

[수집 전략]
- 피크 연도 ±1년 구간: 월별 수집 (월 최대 500건) → 감성 분석 정밀도
- 그 외 연도: 연도별 수집 (연 최대 1,000건) → 전체 맥락 커버

[URL 구조]
뉴스: where=news & pd=3 & ds=YYYY.MM.DD & de=YYYY.MM.DD
블로그: where=blog & nso=so:dd,p:fromYYYYMMDDto YYYYMMDD
  - nso: 네이버 검색 옵션 파라미터
  - so:dd = date descending (최신순)
  - p:fromAto B = A~B 기간 필터

[페이지네이션]
뉴스: start=1, 11, 21 ... (한 페이지 10개)
블로그: start=1, 11, 21 ... (동일)
한 기간당 최대 500건 = 50페이지
"""

import os
import re
import time
import json
import random
import requests
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup

# ── 상수 ──────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

BASE_URL = "https://search.naver.com/search.naver"

# 한 기간당 최대 수집 건수
MAX_PER_PERIOD_PEAK = 500    # 피크 ±1년 구간 (월별)
MAX_PER_PERIOD_OTHER = 1000  # 그 외 구간 (연별)

DATE_PATTERN = re.compile(r"\d{4}\.\s*\d{1,2}\.\s*\d{1,2}")


# ── URL 생성 ──────────────────────────────────────────────────────────────────

def news_url(query: str, ds: str, de: str, start: int) -> str:
    """
    뉴스 검색 URL 생성
    pd=3: 직접 날짜 범위 지정 모드
    ds/de: YYYY.MM.DD 형식
    sort=1: 최신순
    """
    return (
        f"{BASE_URL}?where=news&query={query}"
        f"&pd=3&ds={ds}&de={de}&start={start}&sort=1"
    )


def blog_url(query: str, ds: str, de: str, start: int) -> str:
    """
    블로그 검색 URL 생성
    nso: 네이버 검색 옵션
      so:dd = 최신순 정렬
      p:fromAtoB = A~B 날짜 범위 (YYYYMMDD 형식, 점 없음)
    """
    ds_compact = ds.replace(".", "")  # YYYY.MM.DD → YYYYMMDD
    de_compact = de.replace(".", "")
    return (
        f"{BASE_URL}?where=blog&query={query}"
        f"&nso=so:dd,p:from{ds_compact}to{de_compact}&start={start}"
    )


# ── HTML 파싱 ─────────────────────────────────────────────────────────────────

def parse_news_page(soup: BeautifulSoup) -> list[dict]:
    """
    뉴스 검색 결과 파싱
    n.news.naver.com/mnews/article 패턴의 링크를 기준으로 개별 기사 추출.
    각 링크에서 5단계 위 컨테이너로 올라가서 날짜·제목·설명 파싱.
    """
    results = []
    seen_urls = set()

    for a in soup.find_all("a", href=re.compile(r"n\.news\.naver\.com/mnews/article")):
        href = a["href"]
        if href in seen_urls:
            continue
        seen_urls.add(href)

        # 5단계 위 = 개별 기사 컨테이너
        container = a
        for _ in range(5):
            container = container.parent

        parts = [
            p.strip()
            for p in container.get_text(separator="||").split("||")
            if p.strip()
        ]

        dates = [p for p in parts if DATE_PATTERN.match(p)]
        titles = [
            p for p in parts
            if len(p) > 15
            and not DATE_PATTERN.match(p)
            and "Keep" not in p
            and "저장" not in p
            and "바로가기" not in p
        ]

        results.append({
            "link": href,
            "pubDate": dates[0].replace(" ", "") if dates else None,
            "title": titles[0] if titles else None,
            "description": titles[1] if len(titles) > 1 else None,
        })

    return results


def parse_blog_page(soup: BeautifulSoup) -> list[dict]:
    """
    블로그 검색 결과 파싱
    blog.naver.com/{블로거}/{포스트ID} 패턴 링크 기준으로 파싱.
    """
    results = []
    seen_urls = set()

    for a in soup.find_all("a", href=re.compile(r"blog\.naver\.com/[^/]+/\d+")):
        href = a["href"]
        if href in seen_urls:
            continue
        seen_urls.add(href)

        container = a
        for _ in range(5):
            container = container.parent

        parts = [
            p.strip()
            for p in container.get_text(separator="||").split("||")
            if p.strip()
        ]

        dates = [p for p in parts if DATE_PATTERN.match(p)]
        titles = [
            p for p in parts
            if len(p) > 10
            and not DATE_PATTERN.match(p)
            and "Keep" not in p
            and "저장" not in p
            and "바로가기" not in p
        ]

        results.append({
            "link": href,
            "postdate": dates[0].replace(" ", "") if dates else None,
            "title": titles[0] if titles else None,
            "description": titles[1] if len(titles) > 1 else None,
        })

    return results


# ── 기간별 수집 ───────────────────────────────────────────────────────────────

def scrape_period(
    source: str,      # "news" 또는 "blog"
    keyword: str,
    ds: str,          # 시작일 YYYY.MM.DD
    de: str,          # 종료일 YYYY.MM.DD
    max_items: int,
) -> list[dict]:
    """
    특정 기간의 뉴스 또는 블로그 게시글 수집.

    [페이지네이션 방식]
    start=1  → 1~10번째 결과
    start=11 → 11~20번째 결과
    ...
    start=max_items-9 → 마지막 페이지

    [딜레이 이유]
    요청이 너무 빠르면 네이버가 IP 차단. 랜덤 딜레이로 봇처럼 안 보이게 함.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    all_items = []
    url_fn = news_url if source == "news" else blog_url
    parse_fn = parse_news_page if source == "news" else parse_blog_page

    for start in range(1, max_items + 1, 10):
        url = url_fn(keyword, ds, de, start)

        try:
            res = session.get(url, timeout=10)
            soup = BeautifulSoup(res.text, "html.parser")
        except Exception as e:
            print(f"      요청 실패 (start={start}): {e}")
            break

        items = parse_fn(soup)
        if not items:
            break  # 결과 없으면 더 이상 페이지 없음

        all_items.extend(items)

        if len(items) < 10:
            break  # 마지막 페이지

        # 랜덤 딜레이 (0.8~1.5초): 봇 탐지 회피
        time.sleep(random.uniform(0.8, 1.5))

    return all_items


# ── 기간 목록 생성 ────────────────────────────────────────────────────────────

def build_periods(peak_year: int) -> list[tuple[str, str, int]]:
    """
    수집 기간 목록 생성. (시작일, 종료일, 최대건수) 튜플 리스트 반환.

    [전략]
    - 피크 ±1년 (3년): 월별 분할 → 월 최대 500건
      이유: 감성 분석의 핵심 구간. 월별로 나눠야 1년에 최대 6,000건 수집 가능
    - 2016 ~ 피크-2년: 연도별 → 연 최대 1,000건
    - 피크+2년 ~ 현재: 연도별 → 연 최대 1,000건

    [한 번에 1,000건만 뽑히는 이유]
    네이버 검색 결과는 start 파라미터가 최대 1,000까지만 허용됨.
    기간을 쪼개면 각 기간마다 1,000건씩 뽑을 수 있음.
    """
    periods = []
    current_year = datetime.today().year

    # 피크 구간 이전 연도들 (2016 ~ peak-2)
    for y in range(2016, peak_year - 1):
        if y > current_year:
            break
        periods.append((f"{y}.01.01", f"{y}.12.31", MAX_PER_PERIOD_OTHER))

    # 피크 ±1년: 월별 분할 (peak-1 ~ peak+1)
    start_month = date(peak_year - 1, 1, 1)
    end_month_limit = date(min(peak_year + 1, current_year), 12, 31)

    cur = start_month
    while cur <= end_month_limit:
        month_end = (cur + relativedelta(months=1)) - relativedelta(days=1)
        month_end = min(month_end, end_month_limit)
        periods.append((
            cur.strftime("%Y.%m.%d"),
            month_end.strftime("%Y.%m.%d"),
            MAX_PER_PERIOD_PEAK,
        ))
        cur += relativedelta(months=1)

    # 피크 구간 이후 연도들 (peak+2 ~ 현재)
    for y in range(peak_year + 2, current_year + 1):
        periods.append((f"{y}.01.01", f"{y}.12.31", MAX_PER_PERIOD_OTHER))

    return periods


# ── 키워드 수집 ───────────────────────────────────────────────────────────────

def collect_keyword(
    source: str,
    keyword: str,
    peak_year: int,
    out_dir: str,
) -> pd.DataFrame:
    """
    키워드 하나에 대한 전체 수집 실행.
    이미 수집된 파일이 있으면 재사용 (캐시).
    """
    os.makedirs(out_dir, exist_ok=True)
    periods = build_periods(peak_year)
    all_dfs = []

    for ds, de, max_items in periods:
        # 캐시 파일 경로 (기간별로 저장해서 재실행 시 재사용 가능)
        cache_key = f"{ds}_{de}".replace(".", "")
        cache_path = os.path.join(out_dir, f"{keyword}_{cache_key}.csv")

        if os.path.exists(cache_path):
            all_dfs.append(pd.read_csv(cache_path))
            continue

        items = scrape_period(source, keyword, ds, de, max_items)
        print(f"    {ds}~{de}: {len(items)}건")

        if items:
            df = pd.DataFrame(items)
            df["keyword"] = keyword
            df.to_csv(cache_path, index=False, encoding="utf-8-sig")
            all_dfs.append(df)

        time.sleep(random.uniform(0.5, 1.0))  # 기간 사이 딜레이

    if not all_dfs:
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)

    # link 기준 중복 제거
    date_col = "pubDate" if source == "news" else "postdate"
    combined = combined.drop_duplicates(subset=["link"]).reset_index(drop=True)

    # 날짜 파싱 후 year 컬럼 추가
    combined[date_col] = pd.to_datetime(combined[date_col], format="%Y.%m.%d", errors="coerce")
    combined["year"] = combined[date_col].dt.year

    return combined


# ── 메인 ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    # source 인자: news / blog / both (기본값 both)
    parser.add_argument("--source", choices=["news", "blog", "both"], default="both")
    args = parser.parse_args()

    dl_dir = os.path.join(os.path.dirname(__file__), "../../data/raw/datalab")
    keywords_path = os.path.join(os.path.dirname(__file__), "../../keywords.json")

    with open(keywords_path, "r") as f:
        keywords = [item["keyword"] for item in json.load(f)]

    peak_years = {}
    for kw in keywords:
        dl_path = os.path.join(dl_dir, f"{kw}.csv")
        if not os.path.exists(dl_path):
            continue
        df = pd.read_csv(dl_path)
        df["date"] = pd.to_datetime(df["date"])
        df = df[df["date"].dt.year >= 2016]
        if df.empty:
            continue
        peak_years[kw] = int(df.loc[df["ratio"].idxmax(), "date"].year)

    def run_source(source: str, out_dir: str):
        print(f"\n=== {source.upper()} 스크래핑 시작 ===")
        dfs = []
        for i, kw in enumerate(keywords):
            if kw not in peak_years:
                continue
            peak_year = peak_years[kw]
            print(f"[{i+1}/{len(keywords)}] {kw} (피크: {peak_year})")
            df = collect_keyword(source, kw, peak_year, out_dir)
            if not df.empty:
                df.to_csv(os.path.join(out_dir, f"{kw}.csv"), index=False, encoding="utf-8-sig")
                dfs.append(df)
                print(f"  → 합계 {len(df)}건 (NaN날짜: {df['year'].isna().sum()}건)")
        if dfs:
            combined = pd.concat(dfs, ignore_index=True)
            combined.to_csv(os.path.join(out_dir, "all_keywords.csv"), index=False, encoding="utf-8-sig")
            print(f"\n{source} 완료: 총 {len(combined)}건")

    if args.source in ("news", "both"):
        run_source("news", os.path.join(os.path.dirname(__file__), "../../data/raw/news_scraped"))

    if args.source in ("blog", "both"):
        run_source("blog", os.path.join(os.path.dirname(__file__), "../../data/raw/blog_scraped"))

import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

KEYWORDS = ["불닭", "탕후루"]


def collect_datalab(keywords: list) -> dict:
    """네이버 데이터랩 검색량 수집"""
    url = "https://openapi.naver.com/v1/datalab/search"
    headers = {
        "X-Naver-Client-Id": CLIENT_ID,
        "X-Naver-Client-Secret": CLIENT_SECRET,
        "Content-Type": "application/json"
    }
    body = {
        "startDate": "2021-01-01",
        "endDate": datetime.today().strftime("%Y-%m-%d"),
        "timeUnit": "week",
        "keywordGroups": [{"groupName": kw, "keywords": [kw]} for kw in keywords]
    }
    res = requests.post(url, headers=headers, json=body)
    return res.json()


def collect_news(keyword: str, display: int = 10) -> list:
    """네이버 뉴스 검색"""
    url = "https://openapi.naver.com/v1/search/news.json"
    headers = {
        "X-Naver-Client-Id": CLIENT_ID,
        "X-Naver-Client-Secret": CLIENT_SECRET
    }
    params = {"query": keyword, "display": display, "sort": "date"}
    res = requests.get(url, headers=headers, params=params)
    return res.json().get("items", [])


if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)

    # 데이터랩 수집 + 저장
    print("네이버 데이터랩 수집 중...")
    data = collect_datalab(KEYWORDS)
    rows = []
    for result in data["results"]:
        keyword = result["title"]
        for entry in result["data"]:
            rows.append({"date": entry["period"], "keyword": keyword, "ratio": entry["ratio"]})
    df = pd.DataFrame(rows).pivot(index="date", columns="keyword", values="ratio")
    df.to_csv("data/raw/naver_trends.csv")
    print(f"저장 완료: data/raw/naver_trends.csv ({len(df)}행)")

    # 뉴스 수집 + 저장
    print("\n네이버 뉴스 수집 중...")
    all_news = []
    for kw in KEYWORDS:
        items = collect_news(kw, display=10)
        for item in items:
            item["keyword"] = kw
            all_news.append(item)
    news_df = pd.DataFrame(all_news)
    news_df.to_csv("data/raw/naver_news.csv", index=False)
    print(f"저장 완료: data/raw/naver_news.csv ({len(news_df)}행)")

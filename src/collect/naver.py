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
    # 데이터랩 테스트
    print("네이버 데이터랩 수집 중...")
    data = collect_datalab(KEYWORDS)
    print(data)

    # 뉴스 테스트
    print("\n네이버 뉴스 수집 중...")
    news = collect_news("불닭")
    for item in news[:3]:
        print(item["title"], "|", item["pubDate"])

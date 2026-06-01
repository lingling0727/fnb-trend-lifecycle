import os
import json
import time
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

URL = "https://openapi.naver.com/v1/datalab/search"
START_DATE = "2016-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")


def collect_single(keyword: str) -> pd.DataFrame:
    """키워드 1개 검색량 시계열 수집 — 개별 요청해야 각 키워드가 자체 0-100 기준으로 정규화됨"""
    headers = {
        "X-Naver-Client-Id": CLIENT_ID,
        "X-Naver-Client-Secret": CLIENT_SECRET,
        "Content-Type": "application/json"
    }
    body = {
        "startDate": START_DATE,
        "endDate": END_DATE,
        "timeUnit": "date",
        "keywordGroups": [{"groupName": keyword, "keywords": [keyword]}]
    }
    res = requests.post(URL, headers=headers, json=body)
    data = res.json()

    if "results" not in data:
        print(f"  실패: {keyword} — {data.get('errorMessage', data)}")
        return pd.DataFrame()

    rows = [
        {"date": entry["period"], "keyword": keyword, "ratio": entry["ratio"]}
        for entry in data["results"][0]["data"]
    ]
    return pd.DataFrame(rows)


if __name__ == "__main__":
    keywords_path = os.path.join(os.path.dirname(__file__), "../../keywords.json")
    with open(keywords_path, "r") as f:
        keywords = [item["keyword"] for item in json.load(f)]

    os.makedirs("data/raw/datalab", exist_ok=True)

    all_dfs = []
    for i, kw in enumerate(keywords):
        print(f"[{i+1}/{len(keywords)}] {kw} 수집 중...")
        df = collect_single(kw)
        if not df.empty:
            df.to_csv(f"data/raw/datalab/{kw}.csv", index=False)
            all_dfs.append(df)
        time.sleep(0.3)

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        combined.to_csv("data/raw/datalab/all_keywords.csv", index=False)
        print(f"\n완료: {len(all_dfs)}개 키워드 수집 → data/raw/datalab/")
    else:
        print("수집된 데이터 없음 — API 키 확인 필요")

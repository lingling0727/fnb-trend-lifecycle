import os
import json
import time
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

URL = "https://openapi.naver.com/v1/search/blog.json"
DISPLAY = 100
MAX_ITEMS = 1000
YEARS = list(range(2016, 2027))


def collect_blog_year(keyword: str, year: int) -> pd.DataFrame:
    headers = {
        "X-Naver-Client-Id": CLIENT_ID,
        "X-Naver-Client-Secret": CLIENT_SECRET
    }
    query = f"{keyword} {year}"
    all_items = []
    for start in range(1, MAX_ITEMS + 1, DISPLAY):
        params = {"query": query, "display": DISPLAY, "start": start, "sort": "date"}
        try:
            res = requests.get(URL, headers=headers, params=params, timeout=10)
            data = res.json()
        except Exception as e:
            print(f"    요청 실패 (start={start}): {e}")
            break

        if "items" not in data or not data["items"]:
            break

        for item in data["items"]:
            all_items.append({
                "keyword": keyword,
                "year": year,
                "title": item.get("title", ""),
                "description": item.get("description", ""),
                "bloggername": item.get("bloggername", ""),
                "postdate": item.get("postdate", ""),
                "link": item.get("link", "")
            })

        if len(data["items"]) < DISPLAY:
            break

        time.sleep(0.1)

    return pd.DataFrame(all_items)


if __name__ == "__main__":
    keywords_path = os.path.join(os.path.dirname(__file__), "../../keywords.json")
    with open(keywords_path, "r") as f:
        keywords = [item["keyword"] for item in json.load(f)]

    os.makedirs("data/raw/blog", exist_ok=True)

    all_dfs = []
    for i, kw in enumerate(keywords):
        kw_dfs = []
        print(f"[{i+1}/{len(keywords)}] {kw}")
        for year in YEARS:
            out_path = f"data/raw/blog/{kw}_{year}.csv"
            if os.path.exists(out_path):
                kw_dfs.append(pd.read_csv(out_path))
                continue
            df = collect_blog_year(kw, year)
            if not df.empty:
                df.to_csv(out_path, index=False, encoding="utf-8-sig")
                kw_dfs.append(df)
            print(f"  {year}: {len(df)}건")
            time.sleep(0.3)

        if kw_dfs:
            kw_combined = pd.concat(kw_dfs, ignore_index=True)
            kw_combined.to_csv(f"data/raw/blog/{kw}.csv", index=False, encoding="utf-8-sig")
            all_dfs.append(kw_combined)
            print(f"  → 합계 {len(kw_combined)}건")

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        combined.to_csv("data/raw/blog/all_keywords.csv", index=False, encoding="utf-8-sig")
        print(f"\n완료: 총 {len(combined)}건 → data/raw/blog/")

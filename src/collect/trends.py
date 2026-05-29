from pytrends.request import TrendReq
import pandas as pd
import os

# 분석할 키워드 목록
KEYWORDS = ["불닭", "탕후루", "흑백요리사", "마라탕", "버블티"]

def collect_trends(keywords: list, period: str = "today 5-y") -> pd.DataFrame:
    """Google Trends 검색량 수집"""
    pytrends = TrendReq(hl="ko", tz=540)
    pytrends.build_payload(keywords, timeframe=period, geo="KR")
    df = pytrends.interest_over_time()

    if "isPartial" in df.columns:
        df = df.drop(columns=["isPartial"])

    return df

if __name__ == "__main__":
    print("Google Trends 수집 시작...")
    df = collect_trends(KEYWORDS)
    print(df.tail())

    os.makedirs("data/raw", exist_ok=True)
    df.to_csv("data/raw/trends.csv")
    print(f"\n저장 완료: data/raw/trends.csv ({len(df)}행)")

# -*- coding: utf-8 -*-
"""
YouTube Data API 댓글 수집
- keywords.json의 38개 키워드별로 영상 검색
- 상위 영상의 댓글 수집 (텍스트, 날짜, 좋아요수, 영상 조회수)
- data/raw/youtube/{keyword}_{year}.csv 저장
- 하루 quota: 10,000 units (검색 100 + 댓글 1/page)
"""

import os
import json
import time
import csv
from datetime import datetime
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=API_KEY)

# 설정
MAX_VIDEOS_PER_KEYWORD = 10   # 키워드당 영상 최대 10개
MAX_COMMENTS_PER_VIDEO = 500  # 영상당 댓글 최대 500개 (5페이지)
OUTPUT_DIR = "data/raw/youtube"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# keywords.json 로드
with open("keywords.json", "r", encoding="utf-8") as f:
    keywords = json.load(f)

def search_videos(keyword, max_results=5):
    """키워드로 YouTube 영상 검색 - 100 units 소모"""
    try:
        res = youtube.search().list(
            q=keyword,
            part="id,snippet",
            type="video",
            maxResults=max_results,
            relevanceLanguage="ko",
            regionCode="KR"
        ).execute()
        return res.get("items", [])
    except Exception as e:
        print(f"[검색 실패] {keyword}: {e}")
        return []

def get_video_stats(video_id):
    """영상 조회수 가져오기 - 1 unit 소모"""
    try:
        res = youtube.videos().list(
            part="statistics",
            id=video_id
        ).execute()
        items = res.get("items", [])
        if items:
            return int(items[0]["statistics"].get("viewCount", 0))
    except:
        pass
    return 0

def get_comments(video_id, max_comments=500):
    """영상 댓글 수집 - 1 unit/page, 페이지네이션으로 max_comments까지"""
    comments = []
    next_page_token = None

    try:
        while len(comments) < max_comments:
            kwargs = dict(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                textFormat="plainText",
                order="relevance"
            )
            if next_page_token:
                kwargs["pageToken"] = next_page_token

            res = youtube.commentThreads().list(**kwargs).execute()

            for item in res.get("items", []):
                c = item["snippet"]["topLevelComment"]["snippet"]
                comments.append({
                    "text": c.get("textDisplay", ""),
                    "date": c.get("publishedAt", "")[:10],
                    "likes": c.get("likeCount", 0)
                })

            next_page_token = res.get("nextPageToken")
            # 다음 페이지 없으면 종료
            if not next_page_token:
                break

            time.sleep(0.2)

    except Exception:
        # 댓글 비활성화된 영상은 스킵
        pass

    return comments[:max_comments]

def collect_keyword(keyword):
    """키워드 1개 전체 수집"""
    print(f"[수집 시작] {keyword}")
    rows = []

    videos = search_videos(keyword, MAX_VIDEOS_PER_KEYWORD)
    if not videos:
        print(f"  → 영상 없음, 스킵")
        return

    for video in videos:
        video_id = video["id"].get("videoId")
        if not video_id:
            continue

        title = video["snippet"].get("title", "")
        published = video["snippet"].get("publishedAt", "")[:7]  # YYYY-MM
        view_count = get_video_stats(video_id)
        comments = get_comments(video_id, MAX_COMMENTS_PER_VIDEO)

        print(f"  영상: {title[:30]}... | 댓글 {len(comments)}개 | 조회수 {view_count:,}")

        for c in comments:
            rows.append({
                "keyword": keyword,
                "video_id": video_id,
                "video_title": title,
                "video_published": published,
                "view_count": view_count,
                "comment": c["text"],
                "comment_date": c["date"],
                "comment_likes": c["likes"]
            })

        time.sleep(0.3)  # API 호출 간격

    if rows:
        out_path = os.path.join(OUTPUT_DIR, f"{keyword}.csv")
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"  → {len(rows)}행 저장: {out_path}")

# 메인 수집 루프
print(f"수집 시작: {datetime.now()}")
print(f"키워드 {len(keywords)}개")

for i, kw in enumerate(keywords):
    keyword = kw["keyword"]

    # 이미 수집된 키워드 스킵
    out_path = os.path.join(OUTPUT_DIR, f"{keyword}.csv")
    if os.path.exists(out_path):
        print(f"[스킵] {keyword} - 이미 수집됨")
        continue

    collect_keyword(keyword)

    # quota 소진 방지: 5개마다 잠깐 대기
    if (i + 1) % 5 == 0:
        print(f"--- {i+1}/{len(keywords)} 완료, 5초 대기 ---")
        time.sleep(5)

print(f"\n수집 완료: {datetime.now()}")

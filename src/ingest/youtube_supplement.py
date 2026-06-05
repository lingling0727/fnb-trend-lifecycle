# -*- coding: utf-8 -*-
"""
YouTube 2차 수집 (보완)
- 1차에서 수집된 video_id 제외하고 새 영상만 추가
- 최신 업로드 영상 위주로 탐색 (생애주기 쇠퇴 후 반응 캡처)
- 기존 CSV에 append
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

MAX_VIDEOS_PER_KEYWORD = 10
MAX_COMMENTS_PER_VIDEO = 500
OUTPUT_DIR = "data/raw/youtube"

with open("keywords.json", "r", encoding="utf-8") as f:
    keywords = json.load(f)

def get_collected_video_ids(keyword):
    """기존 CSV에서 이미 수집된 video_id 목록 반환"""
    out_path = os.path.join(OUTPUT_DIR, f"{keyword}.csv")
    ids = set()
    if not os.path.exists(out_path):
        return ids
    with open(out_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if "video_id" in row:
                ids.add(row["video_id"])
    return ids

def search_videos_recent(keyword, max_results=10):
    """최신 업로드순으로 영상 검색"""
    try:
        res = youtube.search().list(
            q=keyword,
            part="id,snippet",
            type="video",
            maxResults=max_results,
            relevanceLanguage="ko",
            regionCode="KR",
            order="date"          # 최신 업로드순
        ).execute()
        return res.get("items", [])
    except Exception as e:
        print(f"[search error] {keyword}: {e}")
        return []

def get_video_stats(video_id):
    try:
        res = youtube.videos().list(part="statistics", id=video_id).execute()
        items = res.get("items", [])
        if items:
            return int(items[0]["statistics"].get("viewCount", 0))
    except:
        pass
    return 0

def get_comments(video_id, max_comments=500):
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
            if not next_page_token:
                break
            time.sleep(0.2)
    except:
        pass
    return comments[:max_comments]

def supplement_keyword(keyword):
    print(f"[2차 수집] {keyword}")

    # 이미 수집된 video_id
    collected_ids = get_collected_video_ids(keyword)
    print(f"  기존 영상 {len(collected_ids)}개 스킵")

    videos = search_videos_recent(keyword, MAX_VIDEOS_PER_KEYWORD)
    new_rows = []

    for video in videos:
        video_id = video["id"].get("videoId")
        if not video_id or video_id in collected_ids:
            continue  # 중복 스킵

        title = video["snippet"].get("title", "")
        published = video["snippet"].get("publishedAt", "")[:7]
        view_count = get_video_stats(video_id)
        comments = get_comments(video_id, MAX_COMMENTS_PER_VIDEO)

        print(f"  + {title[:30]}... | {len(comments)}개 댓글 | {published}")

        for c in comments:
            new_rows.append({
                "keyword": keyword,
                "video_id": video_id,
                "video_title": title,
                "video_published": published,
                "view_count": view_count,
                "comment": c["text"],
                "comment_date": c["date"],
                "comment_likes": c["likes"]
            })

        time.sleep(0.3)

    if new_rows:
        out_path = os.path.join(OUTPUT_DIR, f"{keyword}.csv")
        # append 모드로 기존 파일에 추가
        file_exists = os.path.exists(out_path)
        with open(out_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=new_rows[0].keys())
            if not file_exists:
                writer.writeheader()
            writer.writerows(new_rows)
        print(f"  -> {len(new_rows)}행 추가 완료")
    else:
        print(f"  -> 새 영상 없음")

# 메인
print(f"2차 수집 시작: {datetime.now()}")
for i, kw in enumerate(keywords):
    supplement_keyword(kw["keyword"])
    if (i + 1) % 5 == 0:
        print(f"--- {i+1}/{len(keywords)} 완료 ---")
        time.sleep(3)

print(f"\n2차 수집 완료: {datetime.now()}")

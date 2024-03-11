"""
This script will check if a video is in the download queue

Usage:
    python query-queue.py <video_id>
"""

import os
import sys
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers

load_dotenv()

es = Elasticsearch(
    [os.getenv("ES_HOST")], basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASSWORD"))
)


def fetch_ids(es, index, scroll="2m", size=1000):
    query = {"query": {"match_all": {}}}
    data = helpers.scan(
        es,
        index=index,
        query=query,
        scroll=scroll,
        size=size,
        _source_includes=["youtube_id"],
    )
    for hit in data:
        if "youtube_id" in hit["_source"]:
            yield hit["_source"]["youtube_id"]


def main():
    print("Fetching video ids from download queue")
    download_index = "ta_download"
    queued_videos = set(fetch_ids(es, download_index))
    print(f"Found {len(queued_videos)} video ids in download queue")

    if len(sys.argv) < 2:
        video_id = input("Enter video id: ")
    else:
        video_id = sys.argv[1]

    if video_id in queued_videos:
        print(f"Video {video_id} is in the download queue")
    else:
        print(f"Video {video_id} is not in the download queue")


if __name__ == "__main__":
    main()

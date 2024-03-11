"""
This script will check if a video is in the download queue

Usage:
    python query-queue.py <video_id>
"""

import os
import sys
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from urllib.parse import urlparse, parse_qs

load_dotenv()

es = Elasticsearch(
    [os.getenv("ES_HOST")], basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASSWORD"))
)


def main():
    print("Fetching video ids from download queue")
    download_index = "ta_download"

    ignored = False
    if ignored:
        filter_view = "ignore"
    else:
        filter_view = "pending"

    must_list = [{"term": {"status": {"value": filter_view}}}]

    query = {
        "query": {"bool": {"must": must_list}},
        "sort": [
            {"auto_start": {"order": "desc"}},
            {"timestamp": {"order": "asc"}},
        ],
    }
    iterator = helpers.scan(
        es,
        index=download_index,
        query=query,
        scroll="5m",
        preserve_order=True,
    )
    queued_videos = []
    for video in iterator:
        queued_videos.extend([video["_source"]["youtube_id"]])

    print(f"Found {len(queued_videos)} video ids in download queue")

    if len(sys.argv) < 2:
        video_id = input("Enter video id: ")
    else:
        video_id = sys.argv[1]

    if "youtube.com" in video_id or "youtu.be" in video_id:
        url_data = urlparse(video_id)
        query = parse_qs(url_data.query)
        if url_data.netloc == "youtu.be":
            video_id = url_data.path[1:]
        else:
            if "v" in query:
                video_id = query["v"][0]

    if video_id in queued_videos:
        index = queued_videos.index(video_id)
        print(
            f"Video {video_id} is in the download queue at index {index} out of {len(queued_videos)}"
        )
        query = {"query": {"match": {"youtube_id": video_id}}}
        results = es.search(index=download_index, body=query)
        print(results["hits"]["hits"][0])

    else:
        print(f"Video {video_id} is not in the download queue")


if __name__ == "__main__":
    main()

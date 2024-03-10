"""
This script will remove videos in the download queue that are already present in TA.

Usage:
    python queue-cleanup.py
"""

import os
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
    video_index = "ta_video"
    download_index = "ta_download"
    queued_videos = set(fetch_ids(es, download_index))
    print(f"Found {len(queued_videos)} video ids in download queue")

    print("Fetching video ids already present in TA")
    downloaded_videos = set(fetch_ids(es, video_index))

    duplicates = queued_videos.intersection(downloaded_videos)

    print(
        f"Found {len(duplicates)} duplicates. \nEnsure to make a snapshot in TA before proceeding!!!"
    )

    confirmation = input(
        "Do you want to remove these duplicates from download queue? (yes/no): "
    )
    if confirmation != "yes":
        print("Aborting")
        return

    queries = [
        {"_op_type": "delete", "_index": download_index, "_id": youtube_id}
        for youtube_id in duplicates
    ]

    confirmation = input(
        f"Are you sure you want to remove {len(queries)} duplicates in download queue? (yes/no): "
    )
    if confirmation != "yes":
        print("Aborting")
        return

    print(f"Removing {len(queries)} duplicates")

    (success, failed) = helpers.bulk(es, queries, index=download_index)
    if success > 0:
        print("Successfully removed duplicates.")

    if len(failed) > 0:
        print("Failed to remove some duplicates.")
        print(failed)


if __name__ == "__main__":
    main()

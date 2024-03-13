"""
This script will check if any of the videos in the input fil
is already present in the TA video index, if not it will print the id to the console.

Useful for checking if the videos should be downloaded or not.

Usage:
    python dupe-check.py <input_file>
    <input_file> - File containing list of youtube video URLs or video IDs
"""

import os
import sys
import re
from urllib.parse import urlparse, parse_qs
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv

load_dotenv()

es = Elasticsearch(
    [os.getenv("ES_HOST")], basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASSWORD"))
)


def extract_video_id(video_id=None):
    """Extracts video ID from the string"""
    if "youtube.com" in video_id or "youtu.be" in video_id:
        url_data = urlparse(video_id)
        query = parse_qs(url_data.query)
        if url_data.netloc == "youtu.be":
            video_id = url_data.path[1:]
        else:
            if "v" in query:
                video_id = query["v"][0]
    else:
        id_search = re.search(r"([a-zA-Z0-9_-]{11})", video_id)
        if id_search is not None:
            video_id = id_search.group(1)

    return video_id


def fetch_existing_ids(es, index, video_ids):
    query = {
        "query": {"terms": {"youtube_id": video_ids}}  # List of your local video IDs
    }
    existing_ids = set()
    data = helpers.scan(es, index=index, query=query, _source_includes=["youtube_id"])
    for hit in data:
        existing_ids.add(hit["_source"]["youtube_id"])
    return existing_ids


def main():
    filename = os.path.basename(__file__)
    if len(sys.argv) < 2:
        print(f"Usage: python {filename} <input_file>")
        return

    input_file = sys.argv[1]

    if not os.path.exists(input_file):
        print(f"Input file {input_file} does not exist")
        return

    with open(input_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    local_video_ids = []

    for line in lines:
        video_id = extract_video_id(line)
        if video_id:
            local_video_ids.append(video_id)

    existing_ids_in_index = fetch_existing_ids(es, "ta_video", local_video_ids)

    count = 0
    for video in local_video_ids:
        if not video in existing_ids_in_index:
            count += 1
            print(f"Video {video} is not present in TA")

    print("Script completed successfully!")


if __name__ == "__main__":
    main()

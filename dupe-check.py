"""
This script will check if the specified video id/url or
a file containing list of video ids/urls are already present in TubeArchivist.
If the video is not present, it will print the video id.
If the video is present, it will not print anything

Useful for checking for duplicate videos before adding them to the queue.

Usage:
    python {filename} COMMAND ARG1
    ARG1: Video ID/url or file containing video IDs
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
    print("Fetching existing video IDs from TA...")
    existing_ids = list()
    data = helpers.scan(es, index=index, query=query, _source_includes=["youtube_id"])
    for hit in data:
        existing_ids.append(hit["_source"]["youtube_id"])
    return existing_ids


def main(input_data):
    """Main function"""

    if os.path.exists(input_data):
        with open(input_data, "r", encoding="utf-8") as f:
            video_ids = f.readlines()
            video_ids = [extract_video_id(video_id) for video_id in video_ids]
    else:
        video_ids = [extract_video_id(input_data)]

    existing_ids_in_index = fetch_existing_ids(es, "ta_video", video_ids)

    print("Checking if the videos are already present in TA...")

    for video in video_ids:
        if not video in existing_ids_in_index:
            print(f"Video {video} is not present in TA")

    print("Script completed successfully!")


if __name__ == "__main__":
    filename = os.path.basename(__file__)
    docstring = __doc__.format(filename=filename)

    if len(sys.argv) < 1:
        print(docstring)
        sys.exit(0)

    if sys.argv[1] == "-h":
        print(docstring)
        sys.exit(0)

    user_input = sys.argv[1]

    main(user_input)

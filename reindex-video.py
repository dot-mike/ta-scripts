"""
This script will reindex a video.

Usage:
    python reindex-video.py [<video_id> | <input_file>]
    Specify either:
    <video_id> - A single video ID or the youtube URL
    <input_file> - File containing list of video IDs
"""

import os
import sys
import re
import requests
from urllib.parse import urlparse, parse_qs
from elasticsearch import Elasticsearch
from dotenv import load_dotenv


load_dotenv()

es = Elasticsearch(
    [os.getenv("ES_HOST")], basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASSWORD"))
)

script_dir = os.path.dirname(__file__)

session = requests.Session()
session.verify = False

TA_URL = os.getenv("TA_URL")
API_URL = f"{TA_URL}/api"

session.headers.update(
    {
        "Authorization": f"Token {os.getenv('TA_API_TOKEN')}",
    }
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


def main():
    """Main function"""

    print("Starting script")

    filename = os.path.basename(__file__)
    if len(sys.argv) < 2:
        print(f"Usage: python {filename} [<video_id>|<input_file>]")
        return

    user_input = sys.argv[1]

    if os.path.exists(user_input):
        with open(user_input, "r", encoding="utf-8") as f:
            video_ids = f.readlines()
            video_ids = [extract_video_id(video_id) for video_id in video_ids]

    else:
        video_ids = [extract_video_id(user_input)]

    if not video_ids:
        print("No video IDs found")
        return

    for video_id in video_ids:
        data = {"video": [video_id]}
        r = session.post(f"{API_URL}/refresh", json=data)
        if r.status_code == 200:
            print(f"Reindex video {video_id}")
        else:
            print(f"Failed to reindex video {video_id}: {r.status_code}")

    print("Script completed successfully!")


if __name__ == "__main__":
    main()

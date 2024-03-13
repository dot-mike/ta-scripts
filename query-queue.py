"""
This script will check if a video is in the download queue.

Usage:
    python query-queue.py [<video_id> | <input_file>]
    Specify either:
    <video_id> - A single video ID or the youtube URL
    <input_file> - File containing list of video IDs
"""

import os
import sys
import pickle
import time
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv


load_dotenv()

es = Elasticsearch(
    [os.getenv("ES_HOST")], basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASSWORD"))
)

script_dir = os.path.dirname(__file__)


class VideoChecker:
    """Class to check if a video is in the download queue"""

    def __init__(self, client):
        self.queued_videos = []
        self.client = client
        self.cache_file = Path(script_dir, "queued_videos.pkl")

        self.fetch_queued_videos()

    def fetch_queued_videos(self):
        """Fetches video ids from the download queue"""
        if os.path.exists(self.cache_file):
            cache_time = os.path.getmtime(self.cache_file)
            if (time.time() - cache_time) / 60 < 30:  # 30 minutes
                print("Using ta_download video ids from cache")
                with open(self.cache_file, "rb") as f:
                    self.queued_videos = pickle.load(f)

            return self.queued_videos

        print("Fetching ta_download video ids from Elasticsearch")

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
            self.client,
            index="ta_download",
            query=query,
            scroll="5m",
            preserve_order=True,
        )
        for video in iterator:
            self.queued_videos.extend([video["_source"]["youtube_id"]])

        with open(self.cache_file, "wb") as f:
            print(
                f"Saving {len(self.queued_videos)} video ids to cache: {self.cache_file}"
            )
            pickle.dump(self.queued_videos, f)

        return self.queued_videos

    def check_video_id(self, video_id):
        """Check if video is in the download queue"""

        if video_id in self.queued_videos:
            return self.queued_videos.index(video_id)


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

    checker = VideoChecker(es)
    for video_id in video_ids:
        result = checker.check_video_id(video_id)
        if result is not None:
            print(f"Video {video_id} is in the download queue at position {result}")
        else:
            print(f"Video {video_id} is not in the download queue")

    print("Script completed successfully!")


if __name__ == "__main__":
    main()

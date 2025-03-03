"""
This script queries Elasticsearch for video metadata based on provided video IDs.

Usage:
    python query_ta_videos.py <video_id>
    python query_ta_videos.py <input_file>

Arguments:
    <video_id>   : A single video ID or a YouTube URL.
    <input_file> : A file containing a list of video IDs (one per line).

Example:
    python query_ta_videos.py dQw4w9WgXcQ
    python query_ta_videos.py video_list.txt
"""

import os
import sys
import json
import logging
import re
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

load_dotenv()

ES_HOST = os.getenv("ES_HOST")
ES_USER = os.getenv("ES_USER")
ES_PASSWORD = os.getenv("ES_PASSWORD")

if not ES_HOST or not ES_USER or not ES_PASSWORD:
    print("Error: Elasticsearch environment variables are not set.")
    sys.exit(1)

es = Elasticsearch([
    ES_HOST
], basic_auth=(ES_USER, ES_PASSWORD))

def setup_logging(verbosity):
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, verbosity)]
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")

def extract_video_id(video_id):
    """Extracts video ID from the given input."""
    if "youtube.com" in video_id or "youtu.be" in video_id:
        from urllib.parse import urlparse, parse_qs
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

def query_videos_by_id(video_ids, verbosity=0):
    """Query Elasticsearch for videos by given video IDs."""
    must_query = [{"terms": {"video_id": video_ids}}]
    query_body = {
        "query": {
            "bool": {
                "must": must_query
            }
        }
    }
    
    if verbosity >= 3:
        logging.debug("Elasticsearch Query: %s", json.dumps(query_body, indent=2))
    
    response = es.search(index="ta_video", body=query_body)
    return [hit["_source"] for hit in response["hits"]["hits"]]

def main():
    """Main function to handle CLI input."""
    print("Starting script")
    
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} [<video_id>|<input_file>]")
        sys.exit(1)
    
    user_input = sys.argv[1]
    
    if os.path.exists(user_input):
        with open(user_input, "r", encoding="utf-8") as f:
            video_ids = f.readlines()
            video_ids = [extract_video_id(video_id.strip()) for video_id in video_ids]
    else:
        video_ids = [extract_video_id(user_input)]
    
    if not video_ids:
        print("No valid video IDs found.")
        return
    
    setup_logging(verbosity=1)
    videos = query_videos_by_id(video_ids, verbosity=1)
    
    if videos:
        print(json.dumps(videos, indent=2))
    else:
        print("No matching videos found.")
    
    print("Script completed successfully!")

if __name__ == "__main__":
    main()

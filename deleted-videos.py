"""
This script will let find videos in the index that is Deactivated
"""

import os
import sys
import re
import time
import requests
import click
import dateparser
from click_option_group import (
    optgroup,
    RequiredMutuallyExclusiveOptionGroup,
)
from urllib.parse import urlparse, parse_qs
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv


load_dotenv()

es = Elasticsearch(
    [os.getenv("ES_HOST")], basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASSWORD"))
)

script_dir = os.path.dirname(__file__)

session = requests.Session()

TA_URL = os.getenv("TA_URL")
API_URL = f"{TA_URL}/api"


def process_input_data(input_data):
    # Determine if input_data is a file or a list of video IDs/URLs
    if len(input_data) == 1 and os.path.exists(input_data[0]):
        with open(input_data[0], "r", encoding="utf-8") as f:
            video_ids = [
                vid
                for vid in (extract_video_id(line.strip()) for line in f)
                if vid is not None
            ]
    else:
        video_ids = [extract_video_id(video_str) for video_str in input_data]

    if not video_ids or not any(video_ids):
        print("No valid video IDs provided")
        sys.exit(1)

    return video_ids


def video_exists(video_id):
    r = session.get(f"{API_URL}/video/{video_id}/")
    return r.status_code == 200


def extract_video_id(video_str):
    """
    Extracts video ID from the provided text input or URL.
    """

    video_str = video_str.strip()

    if re.fullmatch(r"[a-zA-Z0-9_-]{11}", video_str):
        return video_str

    if "youtube.com" in video_str or "youtu.be" in video_str:
        url_data = urlparse(video_str)
        if url_data.netloc == "youtu.be":
            return url_data.path[1:]
        else:
            url_query = parse_qs(url_data.query)
            return url_query.get("v", [None])[0]

    return None


@click.group()
def cli():
    """This script lets you add or remove videos from the download queue using the TubeArchivist API."""
    session.headers.update(
        {
            "Authorization": f"Token {os.getenv('TA_API_TOKEN')}",
        }
    )

    if not all(
        [
            os.getenv("TA_URL"),
            os.getenv("TA_API_TOKEN"),
        ]
    ):
        print("Please set all required environment variables. See .env.sample")
        sys.exit(1)


@cli.command()
@click.argument("search_query", nargs=1, required=True, type=str)
def find(search_query):
    """
    Find videos that are deactivated in the index.
    """

    es_query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": search_query,
                            "fields": [
                                "title._2gram",
                                "title._3gram",
                                "title.search_as_you_type",
                            ],
                        }
                    },
                    {"term": {"active": False}},
                ]
            }
        }
    }

    res = es.search(index="ta_video", body=es_query)
    print(res)


# find videos that are deactivated in the index published after a certain date
@cli.command()
@click.option(
    "--published-after",
    help="Filter videos that are published after the provided date",
    required=True,
)
def find_published_after(published_after):
    """
    Find videos that are deactivated and published after a certain date.
    """

    start_date = dateparser.parse(
        published_after,
        languages=["en"],
        settings={
            "PARSERS": [
                "no-spaces-time",
                "timestamp",
                "relative-time",
                "custom-formats",
                "absolute-time",
            ]
        },
    )

    es_query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"active": False}},
                    {
                        "range": {
                            "published": {
                                "gt": start_date.strftime("%Y-%m-%d"),
                            }
                        }
                    },
                ]
            }
        }
    }

    res = es.search(index="ta_video", body=es_query, size="10000", scroll="1m")

    results = res["hits"]

    videos = [
        {
            "id": hit["_id"],
            "title": hit["_source"]["title"],
            "published": hit["_source"]["published"],
            "channel": hit["_source"]["channel"]["channel_name"],
        }
        for hit in results["hits"]
    ]

    for video in videos:
        print(
            f"{video['id']} - {video['title']} - {video['published']} - {video['channel']}"
        )


if __name__ == "__main__":
    cli()

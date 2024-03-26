"""
This script will let you manage the download queue of TubeArchivist.
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
@click.argument("input_data", nargs=-1, type=str)
def add(input_data):
    if not input_data:
        print("No video IDs provided")
        return

    video_ids = process_input_data(input_data)

    unique_video_ids = [
        video_id for video_id in video_ids if not video_exists(video_id)
    ]
    if not unique_video_ids:
        print("All videos already exist in the database")
        return

    data = {
        "data": [
            {
                "youtube_id": "https://www.youtube.com/watch?v=" + video_id,
                "status": "pending",
            }
            for video_id in unique_video_ids
        ]
    }
    r = session.post(f"{API_URL}/download/?autostart=true", json=data)
    r.raise_for_status()
    if r.status_code == 200:
        print(f"Added {len(video_ids)} videos to the download queue")
    print("Response:")
    print(r.json())

    print(
        "Videos will be extracted and added to the download queue shortly.\n"
        "This can take a some minutes depending on the number of videos."
    )


@cli.command()
@click.argument("input_data", nargs=-1, type=str)
def priority(input_data):
    video_ids = process_input_data(input_data)
    if not video_ids:
        return

    for video_id in video_ids:
        print(f"Moving video {video_id} to priority")
        r = session.post(f"{API_URL}/download/{video_id}/", json={"status": "priority"})
        if r.status_code != 200:
            print(f"Failed to move video {video_id} to priority")
        print(r)
        time.sleep(2)


@cli.command()
@click.argument("input_data", nargs=-1, type=str)
@click.option(
    "--only-missing",
    "-m",
    is_flag=True,
    help="Only print the video IDs that are not found",
)
@click.option(
    "--channel",
    "-c",
    is_flag=True,
    help="If you are querying a channel, the video IDs will be extracted from the channel URL.",
)
@click.option(
    "--date",
    is_flag=True,
    help="Query for videos published on a specific date.",
)
@click.option("--json", is_flag=True, help="Get data in JSON format")
@optgroup.group(
    "Query option",
    cls=RequiredMutuallyExclusiveOptionGroup,
    help="Choose if you want to query the download queue(default) or the index.",
)
@optgroup.option(
    "--download-queue",
    "-d",
    is_flag=True,
    help="Query the video status in the download queue.",
)
@optgroup.option(
    "--index", "-i", is_flag=True, help="Query the video status in the index."
)
def query(
    input_data,
    only_missing,
    channel,
    date,
    json,
    download_queue,
    index,
):
    if channel:
        if len(input_data) != 1:
            print("Please provide a channel URL")
            return
        channel_url = input_data[0]
        r = session.get(f"{API_URL}/channel/{channel_url}/")
        if r.status_code != 200:
            print("Channel not found")
            return

        r = session.get(
            f"{API_URL}/download/?channel={channel_url}&filter=pending&page=2"
        )
        if r.status_code != 200:
            print("Failed to query the download queue")
            return

        video_ids = [video["youtube_id"] for video in r.json()["data"]]
        print(f"total hits: {r.json()['paginate']['total_hits']}")

        # pagination
        while r.json()["paginate"]["current_page"] <= r.json()["paginate"]["last_page"]:
            page = r.json()["paginate"]["current_page"] + 1
            r = session.get(
                f"{API_URL}/download/?channel={channel_url}&filter=pending&page={page}"
            )
            if r.status_code != 200:
                print("Failed to query the download queue")
                return
            video_ids.extend([video["youtube_id"] for video in r.json()["data"]])

    elif date:
        if not input_data:
            print("Please provide a date or a range of dates separated by a comma")
            return
        dates = input_data[0].split(",")

        start_date = dateparser.parse(
            dates[0],
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
        print(dates)
        if len(dates) == 1:
            dates.append(dates[0])

        end_date = dateparser.parse(
            dates[1],
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
        if not start_date or not end_date:
            print("Failed to parse date(s)")
            return

        # swap dates if start_date is greater than end_date
        if start_date > end_date:
            start_date, end_date = end_date, start_date

        print(
            f"Querying for videos published between {start_date.date()} and {end_date.date()}"
        )
        # modify the es_query to include a range of dates
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "published": {
                                    "gte": start_date.strftime("%Y-%m-%d"),
                                    "lte": end_date.strftime("%Y-%m-%d"),
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [{"timestamp": {"order": "asc"}}],
        }

        es_result = es.search(
            index="ta_download",
            body=es_query,
            scroll="5m",
            size="10000",
        )
        results = []
        for hit in es_result["hits"]["hits"]:
            results.append(hit["_source"])
        if json:
            print(results)
        else:
            video_ids = [video["youtube_id"] for video in results]
            print("\n".join(video_ids))
        return

    else:
        video_ids = process_input_data(input_data)
        if not video_ids:
            return

    if download_queue:
        url = f"{API_URL}/download"
        query_type = "queue"

    if index:
        url = f"{API_URL}/video"
        query_type = "index"

    missing_videos = []
    found_videos = []
    failed_videos = []

    for video_id in video_ids:
        r = session.get(f"{url}/{video_id}/")
        if r.status_code == 404:
            missing_videos.append(r.json().get("data"))
            continue
        if r.status_code != 200:
            failed_videos.append(r.json().get("data"))
            continue
        else:
            found_videos.append(r.json().get("data"))

    if only_missing:
        print("Missing videos:")
        for video in missing_videos:
            if json:
                print(video)
            else:
                print(video["youtube_id"])
    else:
        print(f"Found videos in the {query_type}:")
        for video in found_videos:
            if json:
                print(video)
            else:
                print(video["youtube_id"])
        print(f"Missing videos in the {query_type}:")
        for video in missing_videos:
            if json:
                print(video)
            else:
                print(video["youtube_id"])
        print(f"Failed videos in the {query_type}:")
        for video in failed_videos:
            if json:
                print(video)
            else:
                print(video["youtube_id"])


if __name__ == "__main__":

    cli()

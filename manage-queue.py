"""
This script will let you manage the download queue of TubeArchivist.
"""

import os
import sys
import re
import time
import requests
import click
from click_option_group import optgroup, RequiredMutuallyExclusiveOptionGroup
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv


load_dotenv()

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
def query(input_data, only_missing, channel, download_queue, index):
    if channel:
        if len(input_data) != 1:
            print("Please provide a channel URL")
            return
        channel_url = input_data[0]
        r = session.get(f"{API_URL}/channel/{channel_url}/")
        if r.status_code != 200:
            print("Channel not found")
            return

        r = session.get(f"{API_URL}/download/?channel={channel_url}&filter=pending")
        if r.status_code != 200:
            print("Failed to query the download queue")
            return

        # {'data': [{'auto_start': False, 'channel_id': 'UC4NbmfkCUDetcXBWWlA4ZxA', 'channel_indexed': False, 'channel_name': '2pluss Info', 'duration': '20s', 'published': '2015-06-16', 'status': 'pending', 'timestamp': 1700093725, 'title': 'Hos psykologen', 'vid_thumb_url': '/cache/videos/l/Lsxjn-xdA0M.jpg', 'vid_type': 'videos', 'youtube_id': 'Lsxjn-xdA0M', '_index': 'ta_download', '_score': 0}]
        video_ids = [video["youtube_id"] for video in r.json()["data"]]
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
            missing_videos.append(video_id)
            continue
        if r.status_code != 200:
            failed_videos.append(video_id)
            continue
        else:
            found_videos.append(video_id)

    if only_missing:
        print("Missing videos:")
        print("\n".join(missing_videos))
    else:
        print(f"Found videos in the {query_type}:")
        print("\n".join(found_videos))
        print(f"Missing videos in the {query_type}:")
        print("\n".join(missing_videos))
        print(f"Failed videos in the {query_type}:")
        print("\n".join(failed_videos))


if __name__ == "__main__":
    cli()

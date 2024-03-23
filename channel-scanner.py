"""
This script index all channels in TubeArchivist and
shows which channels do not have any videos in the database.
"""

import os
import sys
import requests
import click
import time
from click_option_group import optgroup, RequiredMutuallyExclusiveOptionGroup
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


def get_channel_video_info(channel_id, page=0):
    url = f"{API_URL}/channel/{channel_id}/video/?page={page}"
    r = session.get(url)
    if r.status_code != 200:
        return None
    return r.json()


def get_videos_for_channel(channel_id):
    query = {
        "query": {"term": {"channel.channel_id": {"value": channel_id}}},
        "sort": [{"published": {"order": "desc"}}],
    }

    iterator = helpers.scan(
        es,
        index="ta_video",
        query=query,
        scroll="5m",
        preserve_order=True,
    )

    return list(iterator)


def get_videos_in_queue_for_channel(channel_id):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"channel_id": {"value": channel_id}}},
                    {"term": {"status": {"value": "pending"}}},
                ]
            },
        },
        "sort": [
            {"timestamp": {"order": "asc"}},
        ],
    }

    iterator = helpers.scan(
        es,
        index="ta_download",
        query=query,
        scroll="5m",
        preserve_order=True,
    )

    return list(iterator)


@click.group(help=__doc__)
def cli():

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
def dump_all(videos, output):
    """Dump all channels with no videos"""

    # Define the query to get all channel IDs from the channel index
    channel_query = {
        "size": 0,
        "aggs": {
            "unique_channels": {
                "terms": {
                    "field": "channel_id.keyword",
                    "size": 10000,  # Set a size that can accommodate all your channels
                }
            }
        },
    }

    # Execute the query to get all unique channel IDs
    channel_results = es.search(index="ta_channel", body=channel_query)

    # Extract all channel IDs from the channel index
    channel_ids = [
        bucket["key"]
        for bucket in channel_results["aggregations"]["unique_channels"]["buckets"]
    ]

    print("Total channels:", len(channel_ids))

    # Check which channels do not have videos
    channels_without_videos = []

    # Define the query to check if a channel has any videos in the video index
    for channel_id in channel_ids:
        video_query = {"query": {"term": {"channel.channel_id.keyword": channel_id}}}
        # Execute the query
        video_results = es.search(index="ta_video", body=video_query)

        # If the channel has no videos, add it to the list
        if video_results["hits"]["total"]["value"] == 0:
            channels_without_videos.append(channel_id)

    print("Channels without videos:", channels_without_videos)


@cli.command()
@optgroup.group("Input", cls=RequiredMutuallyExclusiveOptionGroup)
@optgroup.option(
    "-c",
    "--channel-id",
    help="Channel ID to scan",
    type=str,
)
@optgroup.option(
    "--channel-ids-file",
    help="Path to a file containing channel IDs",
    type=click.File("r"),
)
def dump_videos(channel_id, channel_ids_file):
    """Get all videos for a channel in the download queue"""
    if not channel_id and not channel_ids_file:
        print("No channel IDs provided")
        return

    channel_ids = []

    if channel_id:
        channel_ids.append(channel_id)

    if channel_ids_file:
        channel_ids.extend(channel_ids_file.read().splitlines())

    for chid in channel_ids:
        print(f"Scanning channel {chid}...")

        videos = get_videos_in_queue_for_channel(chid)
        print(f"Total videos: {len(videos)}")

        # Print the video IDs
        for video in videos:
            print(video["_id"])


@cli.command()
def find_channels():
    """Find all channels with no videos and less than 1000 subscribers with videos in the download queue"""
    channel_query = {
        "size": 0,
        "query": {"range": {"channel_subs": {"lt": 3000}}},
        "aggs": {
            "unique_channels": {
                "terms": {
                    "field": "channel_id",
                    "size": 10000,
                }
            }
        },
    }
    # Execute the query to get all unique channel IDs
    channel_results = es.search(index="ta_channel", body=channel_query)
    # Extract all channel IDs from the channel index
    channel_ids = [
        bucket["key"]
        for bucket in channel_results["aggregations"]["unique_channels"]["buckets"]
    ]

    for channel_id in channel_ids:
        videos = get_videos_for_channel(channel_id)
        if not videos:
            print(f"Channel {channel_id} has no videos in the database")
            # find videos in the download queue
            videos = get_videos_in_queue_for_channel(channel_id)
            if videos and len(videos) > 0 and len(videos) < 40:
                print(
                    f"Channel {channel_id} has {len(videos)} videos in the download queue"
                )
                for video in videos:
                    video_id = video["_id"]
                    r = session.post(
                        f"{API_URL}/download/{video_id}/", json={"status": "priority"}
                    )
                    if r.status_code != 200:
                        print(f"Failed to move video {video_id} to priority")
                    time.sleep(0.5)

                print("Sleeping for 15 seconds...")
                time.sleep(15)


if __name__ == "__main__":
    cli()

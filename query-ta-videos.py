"""
This script is used to query for videos in the archive.

Example usage:
# find all deactivated videos published after X day
- python query-ta-videos.py --active false --published-after 2024-12-01
# get number of deactivated videos per channel
- python query-ta-videos.py --active false --stats | jq '.channels | sort_by(.count)'
"""

import os
import sys
from elasticsearch import Elasticsearch, helpers
import click
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

ES_HOST = os.getenv("ES_HOST")
ES_USER = os.getenv("ES_USER")
ES_PASSWORD = os.getenv("ES_PASSWORD")

if not ES_HOST or not ES_USER or not ES_PASSWORD:
    print("Error: Elasticsearch environment variables are not set.")
    sys.exit(1)

es = Elasticsearch(
    [ES_HOST],
    basic_auth=(ES_USER, ES_PASSWORD)
)

def setup_logging(verbosity):
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, verbosity)]
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")

def convert_to_epoch(date_str):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return int(dt.timestamp())
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Expected format is YYYY-MM-DD.")

def query_videos(
    active=None, video_type=None, published_after=None, published_before=None,
    downloaded_after=None, downloaded_before=None, channel_id=None,
    min_views=None, max_views=None, min_likes=None, max_likes=None,
    max_results=10000, verbosity=0
):
    must_query = []
    filter_query = []
    sort_query = [{"published": {"order": "desc"}}]

    if active is not None:
        must_query.append({"term": {"active": active}})

    if video_type:
        must_query.append({"term": {"vid_type": video_type}})

    if published_after or published_before:
        range_query = {"range": {"published": {}}}
        if published_after:
            range_query["range"]["published"]["gte"] = convert_to_epoch(published_after)
        if published_before:
            range_query["range"]["published"]["lte"] = convert_to_epoch(published_before)
        range_query["range"]["published"]["format"] = "epoch_second"
        filter_query.append(range_query)

    if downloaded_after or downloaded_before:
        range_query = {"range": {"date_downloaded": {}}}
        if downloaded_after:
            range_query["range"]["date_downloaded"]["gte"] = convert_to_epoch(downloaded_after)
        if downloaded_before:
            range_query["range"]["date_downloaded"]["lte"] = convert_to_epoch(downloaded_before)
        range_query["range"]["date_downloaded"]["format"] = "epoch_second"
        filter_query.append(range_query)

    if channel_id:
        must_query.append({"term": {"channel.channel_id": channel_id}})

    if min_views or max_views:
        range_query = {"range": {"stats.view_count": {}}}
        if min_views:
            range_query["range"]["stats.view_count"]["gte"] = min_views
        if max_views:
            range_query["range"]["stats.view_count"]["lte"] = max_views
        filter_query.append(range_query)

    if min_likes or max_likes:
        range_query = {"range": {"stats.like_count": {}}}
        if min_likes:
            range_query["range"]["stats.like_count"]["gte"] = min_likes
        if max_likes:
            range_query["range"]["stats.like_count"]["lte"] = max_likes
        filter_query.append(range_query)

    query_body = {
        "query": {
            "bool": {
                "must": must_query,
                "filter": filter_query
            }
        },
        "sort": sort_query,
        "size": max_results
    }

    if verbosity >= 3:
        logging.debug("Elasticsearch Query: %s", json.dumps(query_body, indent=2))

    response = es.search(index="ta_video", body=query_body)
    return [hit["_source"] for hit in response["hits"]["hits"]]

def calculate_stats(videos):
    """Calculate and display statistics about the queried videos."""
    total_videos = len(videos)
    if total_videos == 0:
        return {"message": "No videos found for the given filters."}

    stats = {
        "total_videos": total_videos,
        "active_videos": sum(1 for v in videos if v.get("active", False)),
        "inactive_videos": sum(1 for v in videos if not v.get("active", False)),
        "total_views": sum(int(v.get("stats", {}).get("view_count", 0) or 0) for v in videos),
        "total_likes": sum(int(v.get("stats", {}).get("like_count", 0) or 0) for v in videos),
        "average_views": 0,
        "average_likes": 0,
        "videos_by_type": {},
        "channels": []  # Updated structure
    }

    stats["average_views"] = stats["total_views"] / total_videos if total_videos > 0 else 0
    stats["average_likes"] = stats["total_likes"] / total_videos if total_videos > 0 else 0

    # Videos by type
    for video in videos:
        vid_type = video.get("vid_type", "unknown")
        stats["videos_by_type"][vid_type] = stats["videos_by_type"].get(vid_type, 0) + 1

    # Collect channel data
    channel_counter = {}
    for video in videos:
        channel_data = video.get("channel", {})
        channel_id = channel_data.get("channel_id", "unknown")
        channel_name = channel_data.get("channel_name", "unknown")
        if channel_id not in channel_counter:
            channel_counter[channel_id] = {"id": channel_id, "text": channel_name, "count": 0}
        channel_counter[channel_id]["count"] += 1

    stats["channels"] = list(channel_counter.values())

    return stats


@click.command()
@click.option("--stats", is_flag=True, help="Display summary statistics instead of full output.")
@click.option("--active", type=bool, help="Filter by video active status (True or False).")
@click.option("--video-type", type=click.Choice(["shorts", "videos"]), help="Filter by video type.")
@click.option("--published-after", type=str, help="Filter videos published after this date (YYYY-MM-DD).")
@click.option("--published-before", type=str, help="Filter videos published before this date (YYYY-MM-DD).")
@click.option("--downloaded-after", type=str, help="Filter videos downloaded after this date (YYYY-MM-DD).")
@click.option("--downloaded-before", type=str, help="Filter videos downloaded before this date (YYYY-MM-DD).")
@click.option("--channel-id", type=str, help="Filter videos by channel ID.")
@click.option("--min-views", type=int, help="Filter videos with minimum number of views.")
@click.option("--max-views", type=int, help="Filter videos with maximum number of views.")
@click.option("--min-likes", type=int, help="Filter videos with minimum number of likes.")
@click.option("--max-likes", type=int, help="Filter videos with maximum number of likes.")
@click.option("--max-results", type=int, default=10000, help="Maximum number of results to fetch.")
@click.option("-v", "--verbose", count=True, help="Increase output verbosity.")
def main(
    stats, active, video_type, published_after, published_before,
    downloaded_after, downloaded_before, channel_id,
    min_views, max_views, min_likes, max_likes, max_results, verbose
):
    setup_logging(verbose)
    videos = query_videos(
        active=active, video_type=video_type, published_after=published_after,
        published_before=published_before, downloaded_after=downloaded_after,
        downloaded_before=downloaded_before, channel_id=channel_id,
        min_views=min_views, max_views=max_views, min_likes=min_likes,
        max_likes=max_likes, max_results=max_results, verbosity=verbose
    )
#    print(json.dumps(results, indent=2))
    if stats:
        video_stats = calculate_stats(videos)
        print(json.dumps(video_stats, indent=2))
    else:
        print(json.dumps(videos, indent=2))

if __name__ == "__main__":
    main()

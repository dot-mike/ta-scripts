""" This script is used to query for videos in the download queue.
The reason for this is to find one or more videos and re-schedule them into the queue by extracting the video ID's and
add all the ID's to the queue again using priority to they downloaded first

Example usage:
# find all videos in download queue with error 'Sign in to confirm you’re not a bot', grab the videos ID's and save them to the file youtube-ids.json
- python query-video-queue.py --filter pending -mf bot_protection -o result.json | jq -r '.[].youtube_id' result.json > youtube-ids.json
# Get some stats about the videos in the queue (max 10000)
- python query-video-queue.py --stats
"""

import os
import sys
from elasticsearch import Elasticsearch, helpers
import json
import click
import logging
from datetime import datetime
from collections import Counter
from dotenv import load_dotenv
import re

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

ERROR_MESSAGE_FILTERS = {
    "player_response_error": "Failed to extract any player response",
    "age_restriction": "Sign in to confirm your age",
    "ip_blocked": "All player responses are invalid. Your IP is likely being blocked by Youtube",
    "hate_speech": "This video has been removed for violating YouTube's policy on hate speech.",
    "video_unavailable": "Video unavailable",
    "account_terminated": "This video is no longer available because the YouTube account associated with this video has been terminated.",
    "private_video": "Private video. Sign in if you've been granted access to this video",
    "copyright_claim": "Video unavailable. This video is no longer available due to a copyright claim by Fiqih Hendra",
    "bot_protection": "Sign in to confirm you’re not a bot"
}

def setup_logging(verbosity):
    """Setup logging configuration based on verbosity level."""
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, verbosity)]
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")

def convert_to_epoch(date_str):
    """Convert YYYY-MM-DD date to epoch_second."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return str(int(dt.timestamp()))
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Expected format is YYYY-MM-DD.")

def query_videos(
    status_filter=None, title_contains=None, no_errors=False, message_filters=None,
    queue_after=None, queue_before=None, published_after=None, published_before=None,
    video_type="videos", max_results=10000, verbosity=0
):
    results = []

    queue_after_epoch = convert_to_epoch(queue_after) if queue_after else None
    queue_before_epoch = convert_to_epoch(queue_before) if queue_before else None

    must_query = []
    should_query = []

    if status_filter:
        must_query.append({"term": {"status": {"value": status_filter}}})

    if title_contains:
        must_query.append({"match": {"title": title_contains}})

    if no_errors:
        must_query.append({"bool": {"must_not": {"exists": {"field": "message"}}}})

    for filter_name in message_filters:
        if filter_name in ERROR_MESSAGE_FILTERS:
            should_query.append({"match_phrase": {"message": {"query": ERROR_MESSAGE_FILTERS[filter_name]}}})

    if video_type:
        must_query.append({"term": {"vid_type": {"value": video_type}}})

    if queue_after_epoch or queue_before_epoch:
        date_range = {"range": {"timestamp": {}}}
        if queue_after_epoch:
            date_range["range"]["timestamp"]["gte"] = queue_after_epoch
        if queue_before_epoch:
            date_range["range"]["timestamp"]["lte"] = queue_before_epoch
        date_range["range"]["timestamp"]["format"] = "epoch_second"
        must_query.append(date_range)

    if verbosity >= 3:
        logging.debug("Elasticsearch Query: %s", json.dumps(body, indent=2))

    body = {
        "query": {
            "bool": {
                "must": must_query,
                "should": should_query,
                "minimum_should_match": 1 if should_query else 0
            }
        },
        "sort": [
            {"auto_start": {"order": "desc"}},
            {"timestamp": {"order": "asc"}}
        ],
        "size": 1000
    }

    scroll_timeout = "2m"
    response = es.search(index="ta_download", body=body, scroll=scroll_timeout)

    scroll_id = response["_scroll_id"]
    hits = response["hits"]["hits"]
    results.extend(hit["_source"] for hit in hits)

    while hits and len(results) < max_results:
        response = es.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
        hits = response["hits"]["hits"]
        results.extend(hit["_source"] for hit in hits)

    return results[:max_results]


def parse_duration(duration_str):
    """Convert a duration string like '15m 32s' into seconds."""
    if not duration_str:
        return 0
    match = re.match(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", duration_str.strip())
    if not match:
        return 0
    hours, minutes, seconds = match.groups()
    total_seconds = (int(hours) * 3600 if hours else 0) + \
                    (int(minutes) * 60 if minutes else 0) + \
                    (int(seconds) if seconds else 0)
    return total_seconds


def calculate_stats(videos):
    """Calculate and display statistics about the results."""
    total_videos = len(videos)
    statuses = Counter(video["status"] for video in videos)
    channels = Counter(video.get("channel_name", "Unknown") for video in videos)
    publication_years = Counter(video["published"][:4] for video in videos if "published" in video)
    error_free = sum(1 for video in videos if "message" not in video)
    with_errors = total_videos - error_free
    with_thumbnails = sum(1 for video in videos if "vid_thumb_url" in video)
    without_thumbnails = total_videos - with_thumbnails

    length_categories = {"short": 0, "medium": 0, "long": 0}
    for video in videos:
        if "duration" in video:
            duration = parse_duration(video["duration"])
            if duration < 300:
                length_categories["short"] += 1
            elif 300 <= duration <= 1800:
                length_categories["medium"] += 1
            else:
                length_categories["long"] += 1

    common_errors = Counter()
    for video in videos:
        message = video.get("message")
        if message:
            for error_key, error_pattern in ERROR_MESSAGE_FILTERS.items():
                if error_pattern in message:
                    common_errors[error_key] += 1

    stats = {
        "total_videos": total_videos,
        "statuses": dict(statuses),
        "channels": dict(channels.most_common(5)),
        "publication_trends": {
            "by_year": dict(publication_years)
        },
        "error_stats": {"error_free": error_free, "with_errors": with_errors},
        "common_errors": dict(common_errors),
        "thumbnail_stats": {"with_thumbnails": with_thumbnails, "without_thumbnails": without_thumbnails},
        "video_length_distribution": length_categories
    }
    return stats


@click.command()
@click.option("--filter", type=str, default=None, help="Filter by status (pending or ignore).")
@click.option("--message-filter", "-mf", type=click.Choice(ERROR_MESSAGE_FILTERS.keys()), multiple=True, help="Filter videos by specific error messages.")
@click.option("--video-type", type=click.Choice(["shorts", "videos"]), default="videos", help="Filter videos by type (default: videos).")
@click.option("--title-contains", type=str, help="Filter videos whose title contains a specific keyword.")
@click.option("--no-errors", is_flag=True, help="Find videos without errors (where 'message' field is null).")
@click.option("--queue-after", type=str, default=None, help="Filter videos added to the queue after this date (YYYY-MM-DD).")
@click.option("--queue-before", type=str, default=None, help="Filter videos added to the queue before this date (YYYY-MM-DD).")
@click.option("--published-after", type=str, default=None, help="Filter videos published after this date (YYYY-MM-DD).")
@click.option("--published-before", type=str, default=None, help="Filter videos published before this date (YYYY-MM-DD).")
@click.option("--max-results", type=int, default=10000, help="Maximum number of results to fetch (default 10,000).")
@click.option("--output", "-o", type=str, default=None, help="Save output to a file (JSON format).")
@click.option("--stats", is_flag=True, help="Display summary statistics instead of full output.")
@click.option("-v", "--verbose", count=True, help="Increase output verbosity. Use -vvv for max verbosity.")
def main(filter, message_filter, video_type, title_contains, no_errors, queue_after, queue_before, published_after, published_before, max_results, output, stats, verbose):
    """
    Query the Elasticsearch video queue with optional filters and pagination.
    """
    setup_logging(verbose)

    logging.info("Starting query with verbosity level: %d", verbose)

    videos = query_videos(
        status_filter=filter,
        message_filters=message_filter,
        video_type=video_type,
        title_contains=title_contains,
        no_errors=no_errors,
        queue_after=queue_after,
        queue_before=queue_before,
        published_after=published_after,
        published_before=published_before,
        max_results=max_results,
        verbosity=verbose
    )

    if stats:
        statistics = calculate_stats(videos)
        print(json.dumps(statistics, indent=2))
    elif output:
        with open(output, "w") as f:
            json.dump(videos, f, indent=2)
        logging.info("Results saved to %s", output)
    else:
        logging.info("No output file specified. Results are not displayed.")

if __name__ == "__main__":
    main()

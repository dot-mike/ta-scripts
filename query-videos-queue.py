import os
import sys
from elasticsearch import Elasticsearch
import json
import click
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

ES_HOST = os.getenv("ES_HOST")
ES_USER = os.getenv("ES_USER")
ES_PASSWORD = os.getenv("ES_PASSWORD")

if not ES_HOST or not ES_USER or not ES_PASSWORD:
    print("Error: Elasticsearch environment variables are not set.")
    sys.exit(1)

# Elasticsearch client
es = Elasticsearch(
    [ES_HOST],
    basic_auth=(ES_USER, ES_PASSWORD)
)

# Predefined custom filters
ERROR_MESSAGE_FILTERS = {
    "player_response_error": "Failed to extract any player response",
    "age_restriction": "Sign in to confirm your age",
    "ip_blocked": "All player responses are invalid. Your IP is likely being blocked by Youtube",
    "hate_speech": "This video has been removed for violating YouTube's policy on hate speech.",
    "video_unavailable": "Video unavailable",
    "account_terminated": "This video is no longer available because the YouTube account associated with this video has been terminated.",
    "private_video": "Private video. Sign in if you've been granted access to this video",
    "copyright_claim": "Video unavailable. This video is no longer available due to a copyright claim by Fiqih Hendra"
}

# Query Elasticsearch for videos
def query_videos(status_filter=None, message_filter=None, start_page=0, end_page=None):
    results = []
    page_size = 12  # Fixed size as required
    current_page = start_page

    while True:
        from_offset = current_page * page_size
        must_query = []

        # Add status filter if specified
        if status_filter:
            must_query.append({"term": {"status": {"value": status_filter}}})
        else:
            must_query.append({"term": {"status": {"value": "pending"}}})

        # Add message filter if specified
        if message_filter:
            must_query.append({"match_phrase": {"message": message_filter}})

        body = {
            "query": {
                "bool": {
                    "must": must_query
                }
            },
            "sort": [
                {"auto_start": {"order": "desc"}},
                {"timestamp": {"order": "asc"}}
            ],
            "size": page_size,
            "from": from_offset
        }

        response = es.search(index="ta_download", body=body)

        hits = response.get("hits", {}).get("hits", [])
        results.extend([hit["_source"] for hit in hits])

        # Stop fetching if there are no more results or the end_page is reached
        if not hits or (end_page is not None and current_page >= end_page):
            break

        current_page += 1

    return results

@click.command()
@click.option("--filter", type=str, default=None, help="Filter by status (pending or ignore).")
@click.option("--message-filter", type=click.Choice(ERROR_MESSAGE_FILTERS.keys()), default=None, help="Filter by specific error messages.")
@click.option("--start-page", type=int, default=0, help="Start page for query, default is 0.")
@click.option("--end-page", type=int, help="End page for query.")
def main(filter, message_filter, start_page, end_page):
    """
    Query the Elasticsearch video queue with optional filters and pagination.
    """
    if filter and filter not in ["pending", "ignore"]:
        print("Error: Filter must be either 'pending' or 'ignore'.")
        sys.exit(1)

    # Get the corresponding message filter value
    message_filter_text = ERROR_MESSAGE_FILTERS.get(message_filter) if message_filter else None

    videos = query_videos(
        status_filter=filter,
        message_filter=message_filter_text,
        start_page=start_page,
        end_page=end_page
    )
    print(json.dumps(videos, indent=2))

if __name__ == "__main__":
    main()

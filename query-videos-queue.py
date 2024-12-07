import os
import sys
from elasticsearch import Elasticsearch
import json
import click
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

def setup_logging(verbosity):
    """Setup logging configuration based on verbosity level."""
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, verbosity)]  # Clamp to available levels
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")

def query_videos(
    status_filter=None, title_contains=None, no_errors=False, message_filters=None,
    queue_after=None, queue_before=None, published_after=None, published_before=None,
    start_page=0, max_pages=None, max_results=None, page_size=12, verbosity=0
):
    results = []
    current_page = start_page
    total_results = 0

    queue_after_epoch = convert_to_epoch(queue_after) if queue_after else None
    queue_before_epoch = convert_to_epoch(queue_before) if queue_before else None

    while True:
        if max_pages is not None and current_page >= max_pages:
            logging.info("Reached max pages limit: %d", max_pages)
            break
        if max_results is not None and total_results >= max_results:
            logging.info("Reached max results limit: %d", max_results)
            break

        from_offset = current_page * page_size
        must_query = []
        should_query = []

        if status_filter:
            must_query.append({"term": {"status": {"value": status_filter}}})

        if title_contains:
            must_query.append({"match": {"title": title_contains}})

        if no_errors:
            must_query.append({"bool": {"must_not": {"exists": {"field": "message"}}}})

        if message_filters:
            should_query = [{"match_phrase": {"message": message}} for message in message_filters]

        if queue_after_epoch or queue_before_epoch:
            date_range = {"range": {"timestamp": {}}}
            if queue_after_epoch:
                date_range["range"]["timestamp"]["gte"] = queue_after_epoch
            if queue_before_epoch:
                date_range["range"]["timestamp"]["lte"] = queue_before_epoch
            date_range["range"]["timestamp"]["format"] = "epoch_second"
            must_query.append(date_range)

        if published_after or published_before:
            date_range = {"range": {"published": {}}}
            if published_after:
                date_range["range"]["published"]["gte"] = published_after
            if published_before:
                date_range["range"]["published"]["lte"] = published_before
            must_query.append(date_range)

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
            "size": page_size,
            "from": from_offset
        }

        if verbosity >= 3:
            logging.debug("Elasticsearch Query: %s", json.dumps(body, indent=2))

        response = es.search(index="ta_download", body=body)
        hits = response.get("hits", {}).get("hits", [])
        results.extend([hit["_source"] for hit in hits])

        total_results += len(hits)
        if len(hits) < page_size:
            break

        current_page += 1

    return results[:max_results] if max_results else results

@click.command()
@click.option("--filter", type=str, default=None, help="Filter by status (pending or ignore).")
@click.option("--title-contains", type=str, help="Filter videos whose title contains a specific keyword.")
@click.option("--no-errors", is_flag=True, help="Find videos without errors (where 'message' field is null).")
@click.option("--queue-after", type=str, default=None, help="Filter videos added to the queue after this date (YYYY-MM-DD).")
@click.option("--queue-before", type=str, default=None, help="Filter videos added to the queue before this date (YYYY-MM-DD).")
@click.option("--published-after", type=str, default=None, help="Filter videos published after this date (YYYY-MM-DD).")
@click.option("--published-before", type=str, default=None, help="Filter videos published before this date (YYYY-MM-DD).")
@click.option("--start-page", type=int, default=0, help="Start page for query, default is 0.")
@click.option("--max-pages", type=int, help="Maximum number of pages to fetch.")
@click.option("--max-results", type=int, help="Maximum number of results to fetch.")
@click.option("--page-size", type=int, default=12, help="Number of items per page.")
@click.option("--output", "-o", type=str, default=None, help="Save output to a file (JSON format).")
@click.option("-v", "--verbose", count=True, help="Increase output verbosity. Use -vvv for max verbosity.")
def main(filter, title_contains, no_errors, queue_after, queue_before, published_after, published_before, start_page, max_pages, max_results, page_size, output, verbose):
    """
    Query the Elasticsearch video queue with optional filters and pagination.
    """
    setup_logging(verbose)

    logging.info("Starting query with verbosity level: %d", verbose)

    videos = query_videos(
        status_filter=filter,
        title_contains=title_contains,
        no_errors=no_errors,
        queue_after=queue_after,
        queue_before=queue_before,
        published_after=published_after,
        published_before=published_before,
        start_page=start_page,
        max_pages=max_pages,
        max_results=max_results,
        page_size=page_size,
        verbosity=verbose
    )

    if output:
        with open(output, "w") as f:
            json.dump(videos, f, indent=2)
        logging.info("Results saved to %s", output)
    else:
        logging.info("No output file specified. Results are not displayed.")

if __name__ == "__main__":
    main()

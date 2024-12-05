import os
import sys
import requests
import json
from tqdm import tqdm
import click
from dotenv import load_dotenv

load_dotenv()

TA_URL = os.getenv("TA_URL")
if not TA_URL:
    print("Error: TA_URL is not set in your .env file.")
    sys.exit(1)

API_URL = f"{TA_URL}/api"
API_TOKEN = os.getenv("TA_API_TOKEN")
if not API_TOKEN:
    print("Error: TA_API_TOKEN is not set in your .env file.")
    sys.exit(1)

session = requests.Session()
session.headers.update({"Authorization": f"Token {API_TOKEN}"})

@click.command()
@click.argument("channel_id", type=str)
@click.option("-o", "--output", type=click.Path(), required=True, help="Output JSON file to write the results.")
def fetch_pending_videos(channel_id, output):
    """
    Fetch all pending videos for a given channel ID.
    """
    print(f"Fetching pending videos for channel: {channel_id}")

    url = f"{API_URL}/download/"
    params = {"channel": channel_id, "filter": "pending", "page": 1}

    all_videos = []
    try:
        while True:
            response = session.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            videos = data.get("data", [])
            all_videos.extend(videos)

            tqdm.write(f"Fetched {len(videos)} videos on page {params['page']}")

            paginate = data.get("paginate", {})
            if not paginate or params['page'] >= paginate.get("last_page", 0):
                break

            params['page'] += 1

    except requests.exceptions.RequestException as e:
        print(f"Error fetching pending videos: {e}")
        sys.exit(1)

    with open(output, "w") as f:
        json.dump(all_videos, f, indent=2)

    print(f"Fetched {len(all_videos)} pending videos. Output written to {output}")

if __name__ == "__main__":
    fetch_pending_videos()

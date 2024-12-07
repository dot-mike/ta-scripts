import os
import sys
import requests
import click
from dotenv import load_dotenv
import json

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

def fetch_pending_videos(max_page):
    """
    Fetch all pending videos up to the specified page.
    """
    all_videos = []
    for page in range(max_page + 1):
        endpoint = f"{API_URL}/download/?filter=pending&page={page}"
        try:
            response = session.get(endpoint)
            if response.status_code == 404:
                print(f"No more results found on page {page}.")
                break
            response.raise_for_status()
            data = response.json()
            videos = data.get("data", [])
            all_videos.extend(videos)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching videos: {e}")
            sys.exit(1)
    return all_videos

@click.command()
@click.argument("max_page", type=int)
def fetch_videos(max_page):
    """
    Command-line tool to fetch all pending videos up to a specified page.
    """
    videos = fetch_pending_videos(max_page)
    print(json.dumps(videos, indent=2))

if __name__ == "__main__":
    fetch_videos()

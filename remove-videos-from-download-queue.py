import os
import sys
import requests
import json
import click
from tqdm import tqdm
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
@click.argument("input_file", type=click.Path(exists=True))
def remove_videos(input_file):
    """
    Remove videos from the download queue based on input file.
    Each line in the input file should contain a YouTube video ID.
    """
    print(f"Reading video IDs from: {input_file}")

    try:
        with open(input_file, "r") as f:
            video_ids = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading input file: {e}")
        sys.exit(1)

    if not video_ids:
        print("No video IDs found in the input file.")
        sys.exit(1)

    print(f"Found {len(video_ids)} video IDs. Removing from the queue...")
    
    for video_id in tqdm(video_ids, desc="Removing videos"):
        endpoint = f"{API_URL}/download/{video_id}/"
        try:
            response = session.delete(endpoint)
            response.raise_for_status()
            tqdm.write(f"Removed video {video_id} successfully.")
        except requests.exceptions.RequestException as e:
            tqdm.write(f"Failed to remove video {video_id}: {e}")

    print(f"Processed {len(video_ids)} video IDs.")

if __name__ == "__main__":
    remove_videos()

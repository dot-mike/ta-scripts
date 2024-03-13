"""
This script will let you add or remove videos from the download queue
using the TubeArchivist API.

Commands:
    add - Add video to the download queue
    remove - Remove video from the download queue
    priority - Move video to priority
    query - Query video status in the download queue

Usage:
    python {filename} COMMAND ARG1
        - COMMAND: Either "add" or "remove"
        - ARG1: Video ID or file containing video IDs
"""

import os
import sys
import re
import requests
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
import time


load_dotenv()

script_dir = os.path.dirname(__file__)

session = requests.Session()

TA_URL = os.getenv("TA_URL")
API_URL = f"{TA_URL}/api"


def video_exists(video_id):
    r = session.get(f"{API_URL}/video/{video_id}/")
    return r.status_code == 200


def extract_video_id(video_id=None):
    """Extracts video ID from the string"""
    if "youtube.com" in video_id or "youtu.be" in video_id:
        url_data = urlparse(video_id)
        query = parse_qs(url_data.query)
        if url_data.netloc == "youtu.be":
            video_id = url_data.path[1:]
        else:
            if "v" in query:
                video_id = query["v"][0]
    else:
        id_search = re.search(r"([a-zA-Z0-9_-]{11})", video_id)
        if id_search is not None:
            video_id = id_search.group(1)

    return video_id


def main(input_data, user_command):
    """Main function"""

    if os.path.exists(input_data):
        with open(input_data, "r", encoding="utf-8") as f:
            video_ids = f.readlines()
            video_ids = [extract_video_id(video_id) for video_id in video_ids]

    else:
        video_ids = [extract_video_id(input_data)]

    if not video_ids:
        print("No video IDs found in the input specified")
        return

    if user_command == "add":
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

    if user_command == "remove":
        for video_id in video_ids:
            print(f"Removing video {video_id} from the download queue")
            r = session.delete(f"{API_URL}/download/{video_id}/")
            if r.status_code != 200:
                print(f"Failed to remove video {video_id} from the download queue")
            time.sleep(2)

    if user_command == "priority":
        for video_id in video_ids:
            print(f"Moving video {video_id} to priority")
            r = session.post(
                f"{API_URL}/download/{video_id}/", json={"status": "priority"}
            )
            if r.status_code != 200:
                print(f"Failed to move video {video_id} to priority")
            print(r)
            time.sleep(2)

    if user_command == "query":
        for video_id in video_ids:
            # first check if the video already exist

            r = session.get(f"{API_URL}/download/{video_id}/")
            if r.status_code == 404:
                print(f"Video {video_id} not found in the download queue")
                continue
            if r.status_code != 200:
                print(f"Failed to query video {video_id} in the download queue")
                continue
            else:
                # print(r.json())
                print(f"Video {video_id} is in the download queue")
            # first check if video is refreshed
            # r = session.get(f"{API_URL}/refresh/?type=video&id={video_id}")
            # if r.status_code != 200:
            #    print(f"Failed to query video {video_id}")
            #    continue

            ## {'total_queued': 593, 'type': 'video', 'id': 'uE-1RPDqJAY', 'state': False}

            # data = r.json()
            # if data["state"] == False:
            #    print(f"Video {video_id} is waiting to be refreshed")
            #    # start refresh
            #    r = session.post(f"{API_URL}/refresh/", json={"video": [video_id]})
            #    print(r)
            #    print(r.json())

            # print(r.json())

    print("Script completed successfully!")


if __name__ == "__main__":
    filename = os.path.basename(__file__)
    docstring = __doc__.format(filename=filename)

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

    if len(sys.argv) < 2:
        print(docstring)
        sys.exit(0)

    if len(sys.argv) < 3:
        if sys.argv[1] == "-h":
            print(docstring)
            sys.exit(0)

        print("Not enough arguments provided.")
        print("Run with -h for help")
        sys.exit(1)

    command = sys.argv[1]
    user_input = sys.argv[2]

    if any([command == "-h", user_input == "-h"]):
        print(docstring)
        sys.exit(0)

    if command not in ["add", "remove", "priority", "query"]:
        print(f"Invalid command: {command}")
        print("Run with -h for help")
        sys.exit(1)

    main(user_input, command)

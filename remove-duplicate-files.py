"""
This script will remove videos in a directory that is already present in TA.
This is useful for cleaning the import directory before importing.

Usage:
    python remove-duplicate-files.py <input_dir>
        input_dir: The directory containing the videos and their associated files.
"""

import os
import sys
import re
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

EXT_MAP = {
    "media": [".mkv", ".webm", ".mp4"],
    "metadata": [".info.json"],
    "thumb": [".jpg", ".png", ".webp"],
    "subtitle": [".vtt"],
    "description": [".description"],
}


es = Elasticsearch(
    [os.getenv("ES_HOST")], basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASSWORD"))
)


def delete_file(file_path):
    try:
        os.remove(file_path)
    except Exception as e:
        print(f"Failed to delete {file_path}: {e}")


def categorize_files(directory):
    """Categorize files based on EXT_MAP and return the desired dictionaries."""
    grouped_files = {}

    # Walk the directory
    for root, dirs, files in os.walk(directory):
        for file in files:
            full_path = os.path.join(root, file)

            # try to extract video id from the first file
            video_id = extract_video_id(file)
            if video_id:
                if video_id not in grouped_files:
                    grouped_files[video_id] = {
                        "media": False,
                        "metadata": False,
                        "thumb": False,
                        "subtitle": False,
                        "video_id": video_id,
                    }
                for category, extensions in EXT_MAP.items():
                    if any(file.endswith(ext) for ext in extensions):
                        if category == "subtitle":
                            if grouped_files[video_id]["subtitle"] is False:
                                grouped_files[video_id]["subtitle"] = []
                            grouped_files[video_id]["subtitle"].append(full_path)
                        else:
                            grouped_files[video_id][category] = full_path

    return list(grouped_files.values())


def extract_video_id(filename):
    """Extracts video ID from the filename which is enclosed in square brackets."""
    base_name, _ = os.path.splitext(filename)
    id_search = re.search(r"\[([a-zA-Z0-9_-]{11})\]", base_name)
    if id_search:
        youtube_id = id_search.group(1)
        return youtube_id

    return None


def fetch_existing_ids(es, index, video_ids):
    query = {
        "query": {"terms": {"youtube_id": video_ids}}  # List of your local video IDs
    }
    existing_ids = set()
    data = helpers.scan(es, index=index, query=query, _source_includes=["youtube_id"])
    for hit in data:
        existing_ids.add(hit["_source"]["youtube_id"])
    return existing_ids


def main():
    filename = os.path.basename(__file__)
    if len(sys.argv) < 2:
        print(f"Usage: python {filename} <input_dir>")
        return

    input_dir = sys.argv[1]

    if not os.path.exists(input_dir):
        print(f"Directory {input_dir} does not exist")
        return

    grouped_videos = categorize_files(input_dir)
    grouped_videos = [
        v for v in grouped_videos if v["media"] and v["video_id"] is not None
    ]

    unique_videos = list({v["video_id"]: v for v in grouped_videos}.values())
    local_video_ids = [v["video_id"] for v in unique_videos]
    print(f"Found {len(unique_videos)} videos to process")

    # Fetch all existing video IDs from the index at once
    existing_ids_in_index = fetch_existing_ids(es, "ta_video", local_video_ids)

    files_to_remove = []

    count = 0
    for video in unique_videos:
        if video["video_id"] in existing_ids_in_index:
            count += 1
            for category, file_path in video.items():
                if category != "video_id" and file_path:
                    if isinstance(file_path, list):
                        files_to_remove.extend(file_path)
                    else:
                        files_to_remove.append(file_path)

    print(f"Deleting {len(files_to_remove)} files for {count} videos in {input_dir}")

    confirmation = input("Are you sure you want to delete these files? (y/n): ")
    if confirmation.lower() != "y":
        print("Aborting")
        return

    with ThreadPoolExecutor(max_workers=10) as executor:
        _ = [executor.submit(delete_file, file_path) for file_path in files_to_remove]
    print("Script completed successfully!")


if __name__ == "__main__":
    main()

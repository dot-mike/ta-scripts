"""
This script will check / repair metadata-files (info.json) that may have missing keys or misspelled keys.
Intended usage is to verify metadata before importing.

Usage:
    python fix-missing-keys-info.py <input_dir>
        input_dir: The directory containing the videos and their associated files.
"""

import os
import sys
import re
import json
from dotenv import load_dotenv

load_dotenv()

EXT_MAP = {
    "media": [".mkv", ".webm", ".mp4"],
    "metadata": [".info.json"],
    "thumb": [".jpg", ".png", ".webp"],
    "subtitle": [".vtt"],
    "description": [".description"],
}


POSSIBLE_MISSING_KEYS = [
    {
        "key": "tags",
        "default": [],
    },
    {
        "key": "categories",
        "default": [],
    },
    {
        "key": "thumbnails",
        "default": [],
    },
    {
        "key": "description",
        "default": "",
    },
    {
        "key": "view_count",
        "default": 0,
    },
    {
        "key": "upload_date",
        "default": "",
    },
    {
        "key": "uploader",
        "default": "",
    },
    {
        "key": "uploader_id",
        "default": "",
    },
    {
        "key": "channel_id",
        "default": "",
    },
    {
        "key": "title",
        "default": "",
    },
]

CORRECT_NAMED_KEYS = [
    {
        "misspelled": ["channelid"],
        "correct": "channel_id",
    },
    {
        "misspelled": ["uploaddate"],
        "correct": "upload_date",
    },
    {
        "misspelled": ["viewdate"],
        "correct": "view_date",
    },
]


def categorize_files(directory):
    """Categorize files based on EXT_MAP and return the desired dictionaries."""
    grouped_files = {}

    for root, _, files in os.walk(directory):
        for file in files:
            full_path = os.path.join(root, file)

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


def main(folder=None):
    filename = os.path.basename(__file__)
    if len(sys.argv) < 2:
        print(f"Usage: python {filename} <input_dir>")
        return

    input_dir = sys.argv[1] if folder is None else folder

    if not os.path.exists(input_dir):
        print(f"Directory {input_dir} does not exist")
        return

    grouped_videos = categorize_files(input_dir)

    # walk through all metadata files and check for missing keys
    for video in grouped_videos:
        metadata_file = video["metadata"]
        if not metadata_file:
            continue

        with open(metadata_file, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        missing_keys = []
        for key in POSSIBLE_MISSING_KEYS:
            if key["key"] not in metadata:
                missing_keys.append(key["key"])

        if missing_keys:
            print(f"Missing keys in {metadata_file}: {missing_keys}")

    print(
        "Finished checking for missing keys in metadata files. If no output was printed, all metadata files are good."
    )


if __name__ == "__main__":
    main()

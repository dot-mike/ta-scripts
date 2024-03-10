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
import subprocess
import json
from dotenv import load_dotenv

# This script validates video files before importing them into TA.

load_dotenv()

EXT_MAP = {
    "media": [".mkv", ".webm", ".mp4"],
    "metadata": [".info.json"],
    "thumb": [".jpg", ".png", ".webp"],
    "subtitle": [".vtt"],
    "description": [".description"],
}


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


def validate_media_file(media_file):
    """Validate media file by running ffprobe on it."""

    command = [
        "ffprobe",
        "-v",
        "error",
        "-of",
        "json",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=codec_type",
        media_file,
    ]

    proc = subprocess.call(command, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    return proc == 0


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

    invalid_media_files = []
    missing_videos = []

    for video in grouped_videos:
        print(
            f"Validating media files: {len(invalid_media_files)}/{len(grouped_videos)}",
            end="\r",
        )
        media_file = video["media"]
        if not media_file:
            missing_videos.append(video["video_id"])
            continue

        if not validate_media_file(media_file):
            invalid_media_files.append(video["video_id"])

    if invalid_media_files:
        print(f"Found {len(invalid_media_files)} invalid media files")
        for video_id in invalid_media_files:
            print(f"{video_id}")

    if missing_videos:
        print(f"Found {len(missing_videos)} videos with missing media files")
        for video_id in missing_videos:
            print(f"{video_id}")

    print("Finished. Data is available in the terminal.")


if __name__ == "__main__":
    main()

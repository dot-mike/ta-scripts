"""
This script will check if there are any missing files for the videos in the input directory.
Intended usage is to verify the import-directory has all the necessary files for each video before importing.

Usage:
    python check-import-missing-data.py <input_dir>
        input_dir: The directory containing the videos and their associated files.
"""

import os
import sys
import re
from dotenv import load_dotenv

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

    missing_videos = []
    # check if any video is missing media, metadata, thumb, or subtitle
    for video in grouped_videos:
        # a video must consist of media + metadata and optionally thumb and subtitle
        if not video["media"] or not video["metadata"]:
            missing_videos.append(video["video_id"])

    # find any single thumb or subtitle without media
    for video in grouped_videos:
        if not video["media"] and (video["thumb"] or video["subtitle"]):
            missing_videos.append(video["video_id"])

    if missing_videos:
        print(f"Found {len(missing_videos)} videos with missing files")
        print("Problematic videos:")
        for video_id in missing_videos:
            print(video_id)

    else:
        print("All videos have the necessary files")


if __name__ == "__main__":
    main()

""" Script to process convert youtube-dl downloads to mp4,
before importing them into TubeArchivist.

Ensure you have a backup of your files before running this script!!!

This script will process all files in the specified directory and convert them to mp4.
it will also do the following:
- extract embedded thumbnails to jpg
- extract subtitles to vtt
- rename files to only contain the video id in the filename
- convert mkv files to mp4
- DELETE old mkv files

Required packages: yt-dlp, pillow, tqdm, ffmpeg_progress_yield

Usage:
    python convert-media-files.py <input_dir>
        input_dir: directory containing the media files to process

Example:
My input directory has the following layout below,
so the command would be: `python3 main.py /channels/Channel_name`

.
├── channels
│   ├── Channel_name
│   │   ├── Video - 20200701
│   │   │   ├── Video [abcdefRU123].mkv
│   │   │   └── Video [abcdefRU123].info.json


Todo:
- Handle vp9 with opus audio in webm files. This requires `-strict -2` flag in ffmpeg command.
- Add support for more video formats?
- Improve error, progress and logging messages.
"""

import json
import os
import re
import shutil
import sys
import subprocess

from ffmpeg_progress_yield import FfmpegProgress
from PIL import Image
from tqdm import tqdm
from yt_dlp.utils import ISO639Utils

EXT_MAP = {
    "media": [".mkv", ".webm"],
    "metadata": [".info.json"],
    "thumb": [".jpg", ".png", ".webp"],
    "subtitle": [".vtt"],
}


def get_streams(media_file):
    """get streams from media file"""
    streams_raw = subprocess.run(
        [
            "ffprobe",
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_streams",
            media_file,
        ],
        capture_output=True,
        check=True,
    )

    streams = json.loads(streams_raw.stdout.decode())

    return streams


def dump_subtitle(idx, media_file, sub_path):
    """extract subtitle from media file"""
    subprocess.run(
        [
            "ffmpeg",
            "-nostats",
            "-loglevel",
            "error",
            "-y",
            "-i",
            media_file,
            "-map",
            f"0:{idx}",
            sub_path,
        ],
        check=True,
    )


def get_mp4_thumb_type(media_file):
    """detect filetype of embedded thumbnail"""
    streams = get_streams(media_file)

    for stream in streams["streams"]:
        if stream["codec_name"] in ["png", "jpg"]:
            return stream["codec_name"]

    return False


def dump_mp4_thumb(media_file, thumb_type):
    """save cover to disk"""
    _, ext = os.path.splitext(media_file)
    new_path = f"{media_file.rstrip(ext)}.{thumb_type}"

    subprocess.run(
        [
            "ffmpeg",
            "-i",
            media_file,
            "-map",
            "0:v",
            "-map",
            "-0:V",
            "-c",
            "copy",
            new_path,
        ],
        check=True,
    )

    return new_path


def get_mkv_thumb_stream(media_file):
    """get cover stream from mkv file"""
    streams = get_streams(media_file)
    attachments = [i for i in streams["streams"] if i["codec_type"] == "attachment"]

    for idx, stream in enumerate(attachments):
        tags = stream["tags"]
        if "mimetype" in tags and tags["filename"].startswith("cover"):
            _, ext = os.path.splitext(tags["filename"])
            return idx, ext

    return None, None


def dump_mpv_thumb(media_file, idx, thumb_type):
    """write cover to disk for mkv"""
    _, media_ext = os.path.splitext(media_file)
    new_path = f"{media_file.rstrip(media_ext)}{thumb_type}"
    subprocess.run(
        [
            "ffmpeg",
            "-v",
            "quiet",
            f"-dump_attachment:t:{idx}",
            new_path,
            "-i",
            media_file,
        ],
        check=False,
    )

    return new_path


def extract_thumbnail(video_dict):
    """Extracts thumbnail from video_dict. If no thumbnail is found, it will try to extract one."""
    if video_dict["thumb"]:
        return video_dict["thumb"]

    media_file = video_dict["media"]
    base_name, ext = os.path.splitext(media_file)

    if os.path.exists(base_name + ".jpg"):
        return base_name + ".jpg"
    elif os.path.exists(base_name + ".png"):
        return base_name + ".png"

    new_path = None

    if ext == ".mkv":
        idx, thumb_type = get_mkv_thumb_stream(media_file)
        if idx is not None:
            new_path = dump_mpv_thumb(media_file, idx, thumb_type)

    elif ext == ".mp4":
        thumb_type = get_mp4_thumb_type(media_file)
        if thumb_type:
            new_path = dump_mp4_thumb(media_file, thumb_type)

    if new_path:
        return new_path


def extract_video_id(filename):
    """Extracts video ID from the filename which is enclosed in square brackets."""
    base_name, _ = os.path.splitext(filename)
    id_search = re.search(r"\[([a-zA-Z0-9_-]{11})\]", base_name)
    if id_search:
        youtube_id = id_search.group(1)
        return youtube_id

    return None


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
                            if grouped_files[video_id]["subtitle"] == False:
                                grouped_files[video_id]["subtitle"] = []
                            grouped_files[video_id]["subtitle"].append(full_path)
                        else:
                            grouped_files[video_id][category] = full_path

    return list(grouped_files.values())


def main():
    """Process files specified in input_dir"""
    if len(sys.argv) < 2:
        print("No input directory specified")
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
    print(f"Found {len(unique_videos)} videos to process")

    with tqdm(total=len(grouped_videos), position=0, leave=True) as pbar:
        for idx, current_video in enumerate(grouped_videos):
            pbar.set_description_str(
                f"Processing {current_video['video_id']} ({idx+1}/{len(grouped_videos)})"
            )
            if not current_video["media"] or current_video["media"].endswith(".mp4"):
                pbar.write(f"Skipping video {current_video['video_id']}")
                continue

            video_id = current_video["video_id"]
            media_path = current_video["media"]
            root_video_path = os.path.dirname(os.path.abspath(current_video["media"]))

            # process thumbnail
            thumb_path = extract_thumbnail(current_video)
            if thumb_path:
                pbar.set_postfix_str("Extracting thumbnail")

                ext = os.path.splitext(thumb_path)[1]
                new_thumb_path = os.path.join(root_video_path, f"[{video_id}].jpg")
                if ext != ".jpg":
                    img_raw = Image.open(thumb_path)
                    img_raw.convert("RGB").save(new_thumb_path)
                    os.remove(thumb_path)
                else:
                    os.rename(thumb_path, new_thumb_path)

            # process subtitles
            streams = get_streams(media_path)
            for idx, stream in enumerate(streams["streams"]):
                pbar.set_postfix_str("Processing subtitles")

                if stream["codec_type"] == "subtitle":
                    lang = ISO639Utils.long2short(stream["tags"]["language"])
                    sub_path = f"[{video_id}].{lang}.vtt"
                    dump_subtitle(
                        idx, media_path, os.path.join(root_video_path, sub_path)
                    )

            # convert to mp4
            pbar.set_postfix_str("Converting video")
            current_video_path = current_video["media"]
            new_video_path = os.path.join(root_video_path, f"[{video_id}].mp4")
            cmd = [
                "ffmpeg",
                "-y",
                "-threads",
                "0",
                "-i",
                current_video_path,
                "-codec",
                "copy",
                new_video_path,
            ]
            with tqdm(
                total=100, position=1, leave=False, desc="Converting video"
            ) as pbar2:
                ff = FfmpegProgress(cmd)
                for progress in ff.run_command_with_progress():
                    pbar2.update(progress - pbar2.n)

            # rename info.json file
            pbar.set_postfix_str("Renaming metadata")
            metadata_path = current_video["metadata"]
            new_metadata_path = os.path.join(root_video_path, f"[{video_id}].info.json")
            os.rename(metadata_path, new_metadata_path)

            # remove old video file
            os.remove(current_video_path)

            pbar.update(1)

    print("Finished processing videos")


if __name__ == "__main__":
    if shutil.which("ffmpeg") is None:
        raise FileNotFoundError("Couldn't find ffmpeg!")

    confirm = input(
        "This script will process all files in the specified directory and convert them to mp4. \
            \nOld video files will be deleted after conversion.\n\nAre you sure you want to continue? (y/n): "
    )
    if confirm.lower() != "y":
        print("Aborted")
        sys.exit(0)

    confirm2 = input(
        "Ensure you have a backup of your files before running this script!!! \
            \n\nAre you sure you want to continue? (y/n): "
    )
    if confirm2.lower() != "y":
        print("Aborted")
        sys.exit(0)

    main()

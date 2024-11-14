"""
A terrible script to export channel metadata with metadata for videos, subtitles, comments and playlist into a ZIP file.
This can be used for exporting backups for a single channel.
Note: I take no responsibility if this breaks your tubearchivist or sets your computer on fire.
"""
import os
import json
import argparse
from datetime import datetime
from zipfile import ZipFile
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv

load_dotenv()
ES_HOST = os.getenv('ES_HOST')
ES_USER = os.getenv('ES_USER')
ES_PASSWORD = os.getenv('ES_PASSWORD')

es = Elasticsearch(
    ES_HOST,
    http_auth=(ES_USER, ES_PASSWORD)
)

def fetch_index_data(index_name, query):
    """Fetch all documents from an index using a scroll to handle large result sets."""
    data = []
    try:
        response = es.search(index=index_name, body=query, scroll="2m", size=1000)
        scroll_id = response["_scroll_id"]
        hits = response["hits"]["hits"]
        data.extend(hits)

        while hits:
            response = es.scroll(scroll_id=scroll_id, scroll="2m")
            hits = response["hits"]["hits"]
            data.extend(hits)
            
    except Exception as e:
        print(f"Error fetching data from {index_name}: {e}")
    
    return [hit["_source"] for hit in data]

def export_ta_format(channel_id, zipf):
    """Export data for each index in TubeArchivist format"""
  
    queries = {
        "ta_channel": {"query": {"term": {"channel_id": channel_id}}},
        "ta_video": {"query": {"term": {"channel.channel_id": channel_id}}},
        "ta_subtitle": {"query": {"term": {"subtitle_channel_id": channel_id}}},
        "ta_comment": {"query": {"term": {"comment_channel_id": channel_id}}},
        "ta_playlist": {"query": {"term": {"playlist_channel_id": channel_id}}}
    }

    for index_name, query in queries.items():
        print(f"Fetching data from {index_name} for TubeArchivist format...")
        data = fetch_index_data(index_name, query)
        file_name = f"{index_name}_{channel_id}.json"
        with open(file_name, "w") as f:
            json.dump(data, f, indent=2)
        zipf.write(file_name, file_name)
        os.remove(file_name)

def export_yt_dlp_format(channel_id, zipf):
    """Export data for each index in yt-dlp compatible-ish format"""

    video_query = {"query": {"term": {"channel.channel_id": channel_id}}}
    videos = fetch_index_data("ta_video", video_query)

    for video in videos:
        video_id = video["youtube_id"]
        
        yt_dlp_video = {
            "title": video.get("title", ""),
            "description": video.get("description", ""),
            "category": video.get("tags", []),
            "vid_thumb_url": video.get("vid_thumb_url", ""),
            "vid_thumb_base64": video.get("vid_thumb_base64", ""),
            "tags": video.get("tags", []),
            "published": video.get("published", ""),
            "vid_last_refresh": video.get("vid_last_refresh", ""),
            "date_downloaded": video.get("date_downloaded", ""),
            "youtube_id": video_id,
            "vid_type": video.get("vid_type", ""),
            "active": True
        }

        # Save yt-dlp compatible video metadata to a separate JSON file
        video_file = f"video_{video_id}.json"
        with open(video_file, "w") as f:
            json.dump(yt_dlp_video, f, indent=2)
        zipf.write(video_file, video_file)
        os.remove(video_file)

        # Fetch subtitles for the video
        subtitle_query = {"query": {"term": {"youtube_id": video_id}}}
        subtitles = fetch_index_data("ta_subtitle", subtitle_query)
        subtitle_file = f"subtitles_{video_id}.json"
        with open(subtitle_file, "w") as f:
            json.dump(subtitles, f, indent=2)
        zipf.write(subtitle_file, subtitle_file)
        os.remove(subtitle_file)

        # Fetch comments for the video
        comment_query = {"query": {"term": {"youtube_id": video_id}}}
        comments = fetch_index_data("ta_comment", comment_query)
        comment_file = f"comments_{video_id}.json"
        with open(comment_file, "w") as f:
            json.dump(comments, f, indent=2)
        zipf.write(comment_file, comment_file)
        os.remove(comment_file)

def export_channel_backup(channel_id, output_filename, format_choice):
    with ZipFile(output_filename, 'w') as zipf:
        if format_choice in ["ta", "both"]:
            export_ta_format(channel_id, zipf)
        
        if format_choice in ["yt-dlp", "both"]:
            export_yt_dlp_format(channel_id, zipf)
    
    print(f"Backup completed and saved to {output_filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export channel data as a structured backup.")
    parser.add_argument("channel_id", help="The ID of the channel to export")
    parser.add_argument("--format", choices=["ta", "yt-dlp", "both"], default="both", help="Backup format: TubeArchivist (ta), yt-dlp (yt-dlp), or both (default: both)")
    args = parser.parse_args()
    
    date_str = datetime.now().strftime("%Y%m%d")
    output_filename = f"ta-backup-{args.channel_id}_{date_str}.zip"
    
    export_channel_backup(args.channel_id, output_filename, args.format)

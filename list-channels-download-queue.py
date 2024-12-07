import os
import sys
import json
import click
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

load_dotenv()

ES_HOST = os.getenv("ES_HOST")
ES_USER = os.getenv("ES_USER")
ES_PASSWORD = os.getenv("ES_PASSWORD")

if not ES_HOST or not ES_USER or not ES_PASSWORD:
    print("Error: Elasticsearch configuration is not set in your .env file.")
    sys.exit(1)

es = Elasticsearch([ES_HOST], basic_auth=(ES_USER, ES_PASSWORD))

@click.command()
@click.option('--videos', is_flag=True, help="Include a list of video IDs for each channel.")
def list_channels_download_queue(videos):
    """
    Fetch and display all channels in the download queue along with the total count of videos.
    Optionally include video IDs with the --videos flag.
    """
    index_name = "ta_download"
    query = {
        "query": {
            "term": {"status": "pending"}
        },
        "aggs": {
            "channel_count": {
                "terms": {
                    "field": "channel_id",
                    "size": 10000
                }
            }
        },
        "size": 0
    }

    try:
        response = es.search(index=index_name, body=query)
        buckets = response.body.get("aggregations", {}).get("channel_count", {}).get("buckets", [])

        if not buckets:
            print(json.dumps([], indent=2))
            return

        # Build JSON output
        output = []
        for bucket in buckets:
            channel_data = {
                "channel_id": bucket["key"],
                "count": bucket["doc_count"]
            }
            if videos:
                # Fetch videos for the channel
                video_query = {
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"channel_id": bucket["key"]}},
                                {"term": {"status": "pending"}}
                            ]
                        }
                    },
                    "_source": ["youtube_id"],
                    "size": 10000
                }
                video_response = es.search(index=index_name, body=video_query)
                video_ids = [hit["_source"]["youtube_id"] for hit in video_response.body["hits"]["hits"]]
                channel_data["videos"] = video_ids

            output.append(channel_data)

        print(json.dumps(output, indent=2))

    except Exception as e:
        print(f"Error executing Elasticsearch query: {e}")

if __name__ == "__main__":
    list_channels_download_queue()

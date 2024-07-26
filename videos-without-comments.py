"""
This script find all videos without comments
and write the IDs to file video_ids_without_comments.txt
"""

import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
import logging
import dateparser

es_logger = logging.getLogger("elasticsearch")
es_logger.setLevel(logging.DEBUG)

load_dotenv()

es = Elasticsearch(
    [os.getenv("ES_HOST")], basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASSWORD"))
).options(ignore_status=404)

es_query = {
    "size": 10000,
    "query": {
        "bool": {
            "must_not": [
                {"exists": {"field": "comment_count"}},
                {"range": {"comment_count": {"gt": 0}}},
            ],
            "must": [
                {"term": {"vid_type": "videos"}},
                # {
                #    "range": {
                #        "vid_last_refresh": {
                #            "gte": str(int(dateparser.parse("2024-01-01").timestamp())),
                #            "lte": str(int(dateparser.parse("2024-12-31").timestamp())),
                #        }
                #    }
                # },
                # {"range": {"stats.view_count": {"gt": 1000}}},
            ],
        }
    },
    "_source": ["youtube_id", "published", "vid_type", "title"],
}

response = es.search(index="ta_video", body=es_query, scroll="5m")

scroll_id = response["_scroll_id"]

results = response["hits"]["hits"]

while len(response["hits"]["hits"]):
    response = es.scroll(scroll_id=scroll_id, scroll="5m")
    results.extend(response["hits"]["hits"])

video_ids = [hit["_source"]["youtube_id"] for hit in results]

with open("video_ids_without_comments.txt", "w") as file:
    for video_id in video_ids:
        file.write(f"{video_id}\n")

print(f"Total videos without comments: {len(video_ids)}")
print("Video IDs have been written to video_ids_without_comments.txt")

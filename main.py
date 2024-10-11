import json
import time
from elasticsearch import Elasticsearch
from src.load import conn, init
from bson import json_util

def parse_json(data):
    return json.loads(json_util.dumps(data))

def push_data():
    init()
    collName,_ = conn()

    # Create the client instance
    client = Elasticsearch(
        "http://localhost:9200"
    )

    docu = collName.find()

    for doc in docu:
        doc_json = parse_json(doc)
        doc_json_id = doc_json.pop("_id")
        client.index(index="velib", id=doc_json_id, body=doc_json)

    print("Indexation terminée.")

while True:
    push_data()
    time.sleep(60) 


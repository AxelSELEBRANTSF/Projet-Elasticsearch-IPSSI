import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pymongo import MongoClient
from bson import json_util

def parse_json(data):
    return json.loads(json_util.dumps(data))

def push_data(collection_name: str):
    print("Starting push_data function")
    try:
        client = MongoClient("mongodb://localhost:27017")
        db = client["Velib"]
        collName = db[collection_name]
        print(f"Successfully connected to MongoDB, using collection: {collection_name}")

        # Create the Elasticsearch client instance
        try:
            es_client = Elasticsearch("http://localhost:9200")
            print("Successfully created Elasticsearch client")
        except Exception as es_error:
            print(f"Error creating Elasticsearch client: {es_error}")
            return

        docu = collName.find()
        doc_count = collName.count_documents({})
        print(f"Found {doc_count} documents in the collection")

        # Ensure the index exists
        if not es_client.indices.exists(index="velib"):
            es_client.indices.create(index="velib")
            print("Created index 'velib' in Elasticsearch")

        def generate_actions():
            for doc in docu:
                doc_json = parse_json(doc)
                doc_json_id = str(doc_json.pop("_id")['$oid'])
                yield {
                    '_op_type': 'index',  # This will create or update the document
                    '_index': 'velib',
                    '_id': doc_json_id,
                    '_source': doc_json
                }

        # Use bulk API for better performance
        success, failed = bulk(es_client, generate_actions())
        print(f"Indexed {success} documents successfully. {failed} documents failed.")

    except Exception as e:
        print(f"An error occurred in push_data: {e}")


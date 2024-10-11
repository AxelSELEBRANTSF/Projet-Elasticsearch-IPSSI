import calendar
import time
from typing import Any, Dict, List, Optional
from pymongo.collection import Collection
from pymongo import MongoClient
import requests
from requests.exceptions import HTTPError

def get_data(offset: int) -> Optional[List[Dict[str, Any]]]:
    try:
        r = requests.get(f"https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit=100&offset={offset}")
        r.raise_for_status()
        jsonResponse = r.json()
        return jsonResponse.get("results")
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    return None

def conn() -> tuple[Collection, str]:
    CONNECTION_STRING = "mongodb://localhost:27017"  
    client = MongoClient(CONNECTION_STRING)
    current_GMT = time.gmtime()
    ts = calendar.timegm(current_GMT)
    db = client["Velib"]
    collection_name = f"velo_libre_{ts}"
    collection = db[collection_name]
    return collection, collection_name

def set_data(client: Collection, data: List[Dict[str, Any]]) -> Any:
    try:
        result = client.insert_many(data)
        return result
    except Exception as err:
        print(f"Error in set_data: {err}")
        raise

def get_total_count() -> Optional[int]:
    try:
        r = requests.get(f"https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit=1&offset=0")
        r.raise_for_status()
        jsonResponse = r.json()
        return jsonResponse.get("total_count")
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    return None

def init() -> str:
    print("Starting init function")
    collection, collection_name = conn()
    print(f"Connected to MongoDB. Using collection: {collection_name}")

    total_count = get_total_count()
    print(f"Total count of records to fetch: {total_count}")

    if total_count is not None:
        records_inserted = 0
        for offset in range(0, total_count, 100):
            print(f"Fetching data with offset: {offset}")
            data = get_data(offset)
            if data:
                print(f"Fetched {len(data)} records. Inserting into MongoDB...")
                try:
                    result = set_data(collection, data)
                    records_inserted += len(data)
                    print(f"Inserted {len(data)} records. Total inserted: {records_inserted}")
                except Exception as e:
                    print(f"Error inserting data: {e}")
            else:
                print(f"No data fetched for offset: {offset}")
    else:
        print("Failed to get total count of records")

    final_count = collection.count_documents({})
    print(f"Final count of documents in collection {collection_name}: {final_count}")
    
    return collection_name

    #map_function = Codec("""
    #function() {
    #    emit(this.nom_arrondissement_communes, 1);
    #}
    #""")

    #reduce_function = Codec("""
    #function(key, values) {
    #    return Array.sum(values);
    #}
    #""")

    #result = collection.database.command(
     #   'mapReduce',
      #  collection_name,
       # map=map_function,
        #reduce=reduce_function,
        #out={'inline': 1}
    #)

    # Sort the results
    #sorted_results = sorted(result['results'], key=lambda x: x['value'], reverse=True)

    # Print the results
    #for doc in sorted_results:
     #   print(f"Arroundissement: {doc['_id']}, Nombre de vélib disponible: {doc['value']}")

if __name__ == "__main__":
    init()
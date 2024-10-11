from src.load import init
from main import push_data
import time

def main():
    print("Starting data collection...")
    start_time = time.time()
    collection_name = init()
    end_time = time.time()
    print(f"Data collection completed in {end_time - start_time:.2f} seconds.")
    
    print("Waiting for 5 seconds before pushing data...")
    time.sleep(5)  # Add a small delay to ensure data is fully committed to MongoDB
    
    print("Starting data push to Elasticsearch...")
    push_data(collection_name)
    print("Data push completed.")

if __name__ == "__main__":
    while True:
        main()
        time.sleep(60)
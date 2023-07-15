from elasticsearch import Elasticsearch
import time
import os

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# Specify the index name
index_name = 'web_indexer'

# Define the initial size of the index
initial_size = 0

# Initialize the rate of size increase to 0
size_increase_rate_mb_min = 0

# Get the path to the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

while True:
    # Get the current size of the index
    index_stats = es.indices.stats(index=index_name)
    index_size_bytes = index_stats['_all']['total']['store']['size_in_bytes']
    index_size_mb = index_size_bytes / (1024 * 1024)
    
    # Calculate the rate of size increase
    if index_size_mb > initial_size:
        size_increase_rate_mb_min = (index_size_mb - initial_size) / 1.0
        print(f"Index size increased to: {index_size_mb:.2f} MB")
        print(f"Rate of size increase: {size_increase_rate_mb_min:.2f} MB/min")
        initial_size = index_size_mb
    
    # Calculate the time till the next check (every minute)
    time_till_next_check = 60 - (time.time() % 60)
    print(f"Time till next check: {time_till_next_check:.2f} seconds")

    # wait for 2 seconds before the next iteration
    time.sleep(2)

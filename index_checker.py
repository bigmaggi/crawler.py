from elasticsearch import Elasticsearch
import time

# Connect to Elasticsearch
es = Elasticsearch([{'host': '85.215.101.248', 'port': 9200, 'scheme': 'http'}])

# Specify the index name
index_name = 'web_indexer'

# Define the initial size of the index
initial_size = 0

# Initialize the rate of size increase to 0
size_increase_rate_mb_min = 0

while True:
    # Get the current size of the index
    try:
        index_stats = es.indices.stats(index=index_name)
        index_size_bytes = index_stats['_all']['total']['store']['size_in_bytes']
        index_size_mb = index_size_bytes / (1024 * 1024)
    except KeyError:
        # Handle the KeyError if the 'store' key is not present
        print("Error: Unable to retrieve index size.")
        time.sleep(10)
        continue
    
    # Calculate the rate of size increase
    if index_size_mb > initial_size:
        size_increase_rate_mb_min = (index_size_mb - initial_size) / 1.0
        print(f"Index size increased to: {index_size_mb:.2f} MB")
        print(f"Rate of size increase: {size_increase_rate_mb_min:.2f} MB/min")
        initial_size = index_size_mb
    
        # Calculate the estimated size in 1 hour and 24 hours
        size_increase_rate_mb_hour = size_increase_rate_mb_min * 60
        estimated_size_1_hour = index_size_mb + size_increase_rate_mb_hour
        estimated_size_24_hours = index_size_mb + size_increase_rate_mb_hour * 24

        print(f"Estimated size in 1 hour: {estimated_size_1_hour:.2f} MB")
        print(f"Estimated size in 24 hours: {estimated_size_24_hours:.2f} MB")
    
    # Calculate the time till the next check (every 10 seconds)
    time_till_next_check = 10 - (time.time() % 10)
    print(f"Time till next check: {time_till_next_check:.2f} seconds")

    # Wait for 10 seconds before the next iteration
    time.sleep(time_till_next_check)


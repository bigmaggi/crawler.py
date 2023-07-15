from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# Specify the index name
index_name = 'web_indexer'

# Get index size in bytes
index_stats = es.indices.stats(index=index_name)
index_size_bytes = index_stats['_all']['total']['store']['size_in_bytes']

# Convert bytes to megabytes
index_size_mb = index_size_bytes / (1024 * 1024)

print(f"Size of index '{index_name}': {index_size_mb:.2f} MB")


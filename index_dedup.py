from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

# Connect to Elasticsearch
es = Elasticsearch([{'host': '202.61.236.229', 'port': 9200, 'scheme': 'http'}])

# Specify the index name
index_name = 'web_indexer'

# Define the query to retrieve all documents
query = {
    "query": {
        "match_all": {}
    }
}

# Perform a scan query to retrieve all documents in the index
documents = scan(es, index=index_name, query=query)

# Create a dictionary to track seen document IDs
seen_documents = {}

# Iterate over the documents and identify duplicates
duplicate_count = 0
for doc in documents:
    document_id = doc['_id']

    # Check if the document ID has been seen before
    if document_id in seen_documents:
        duplicate_count += 1

        # Delete the duplicate document
        es.delete(index=index_name, id=document_id)
    else:
        # Mark the document ID as seen
        seen_documents[document_id] = True

print(f"Total duplicates removed from '{index_name}': {duplicate_count}")


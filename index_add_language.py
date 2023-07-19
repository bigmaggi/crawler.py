from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from langdetect import detect

ELASTICSEARCH_HOST = "202.61.236.229"
ELASTICSEARCH_PORT = 9200
ELASTICSEARCH_INDEX = "web_indexer"

def add_language_to_documents():
    es = Elasticsearch([{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}])

    # Query all documents that do not have a language field
    body = {
        "query": {
            "bool": {
                "must_not": {
                    "exists": {
                        "field": "language"
                    }
                }
            }
        }
    }

    # Execute the search
    resp = es.search(index=ELASTICSEARCH_INDEX, body=body, scroll='1m')

    # Get the scroll ID from the response, which we'll use to scroll through all the results
    scroll_id = resp['_scroll_id']

    while len(resp['hits']['hits']):
        bulk_body = []

        # Process each document
        for doc in resp['hits']['hits']:
            try:
                language = detect(doc['_source']['content'])
            except:
                language = 'unknown'

            # Prepare the Update API request
            update_request = {
                "_op_type": "update",
                "_index": ELASTICSEARCH_INDEX,
                "_id": doc['_id'],
                "doc": {
                    "language": language
                }
            }

            bulk_body.append(update_request)

        # Execute the Update By Query API request
        bulk(  # using the helper function
            client=es,
            actions=bulk_body,
        )

        # Scroll to get the next batch of results
        resp = es.scroll(scroll_id=scroll_id, scroll='1m')

if __name__ == "__main__":
    add_language_to_documents()


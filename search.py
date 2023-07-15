from elasticsearch import Elasticsearch

def search_documents(es, index, query, limit=20):
    """
    Searches documents in Elasticsearch.

    Args:
        es (Elasticsearch): Elasticsearch client.
        index (str): The name of the index to search.
        query (str): The query string.
        limit (int): The number of results to return.

    Returns:
        list: A list of search results.
    """
    response = es.search(index=index, body={
        "query": {
            "match": {
                "content": query
            }
        },
        "size": limit
    })

    return response['hits']['hits']

def main():
    # Connect to Elasticsearch
    es = Elasticsearch(
        [{"host": "localhost", "port": 9200, "scheme": "http"}]
    )

    # Check if index exists
    if not es.indices.exists(index="web_indexer"):
        # Create index
        es.indices.create(index="documents")

    # Search for a query
    query = input("Enter a search query: ")
    search_results = search_documents(es, "web_indexer", query)
    print("Search Results:")
    for result in search_results:
        print(f"URL: {result['_source']['url']}")
        print(f"Score: {result['_score']}")
        print()

if __name__ == "__main__":
    main()

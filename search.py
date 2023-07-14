from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from sklearn.feature_extraction.text import TfidfVectorizer
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import numpy as np

# Elasticsearch configuration
ELASTICSEARCH_HOST = "localhost"
ELASTICSEARCH_PORT = 9200
ELASTICSEARCH_INDEX = "web_indexer"

# Create an Elasticsearch client
client = Elasticsearch(
    hosts=[{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}]
)

# Search the documents in Elasticsearch
def search_documents(client, query, limit=10, num_threads=16):
    # Get all documents from Elasticsearch
    documents = scan(client, index=ELASTICSEARCH_INDEX, query={"query": {"match_all": {}}})

    # Extract the URLs and contents from the documents
    urls = []
    contents = []
    for doc in documents:
        url = doc["_source"].get("url")
        content = doc["_source"].get("content")
        if url and content:
            urls.append(url)
            contents.append(content)

    if not urls:
        print("No documents found.")
        return []

    # Apply TF-IDF vectorization to the contents
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(contents)

    # Normalize the query vector
    query_vector = vectorizer.transform([query])
    query_vector_normalized = query_vector / np.linalg.norm(query_vector.toarray())

    # Calculate the cosine similarity between the normalized query vector and document vectors using multiple threads
    cosine_similarities = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in tqdm(range(len(urls)), desc="Calculating similarities"):
            document_vector_normalized = X[i] / np.linalg.norm(X[i].toarray())
            future = executor.submit(cosine_similarity, query_vector_normalized, document_vector_normalized)
            futures.append(future)
        for i, future in enumerate(tqdm(futures, desc="Collecting results", total=len(futures))):
            similarity = future.result()
            cosine_similarities.append((urls[i], similarity))

    # Sort the results by similarity
    sorted_results = sorted(cosine_similarities, key=lambda x: x[1], reverse=True)

    # Return the top 'limit' search results with scores
    return sorted_results[:limit]

# Calculate the cosine similarity between two vectors
def cosine_similarity(v1, v2):
    dot_product = v1.dot(v2.T)
    return dot_product[0, 0]

def main():
    # Search for a query
    query = input("Enter a search query: ")
    search_results = search_documents(client, query, limit=10, num_threads=16)
    print("Search Results:")
    for rank, (result, score) in enumerate(search_results, start=1):
        print(f"Rank: {rank}")
        print(f"URL: {result}")
        print(f"Score: {score}")
        print()

if __name__ == "__main__":
    main()


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

# Okapi BM25 parameters
K1 = 1.2
B = 0.75

# Create an Elasticsearch client
client = Elasticsearch(
    hosts=[{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}]
)

# Search the documents in Elasticsearch using Okapi BM25 ranking
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

    # Calculate the Okapi BM25 similarity between the normalized query vector and document vectors using multiple threads
    bm25_similarities = []
    avgdl = np.mean(X.sum(axis=1))
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in tqdm(range(len(urls)), desc="Calculating similarities"):
            document_vector = X[i].toarray()
            document_length = np.sum(document_vector)
            document_vector_normalized = document_vector / document_length
            future = executor.submit(okapi_bm25_similarity, query_vector_normalized.toarray(), document_vector_normalized, document_length, avgdl)
            futures.append(future)
        for i, future in enumerate(tqdm(futures, desc="Collecting results", total=len(futures))):
            similarity = future.result()
            bm25_similarities.append((urls[i], similarity))

    # Sort the results by similarity
    sorted_results = sorted(bm25_similarities, key=lambda x: x[1], reverse=True)

    # Return the top 'limit' search results with scores
    return sorted_results[:limit]

# Calculate the Okapi BM25 similarity between the query vector and document vector
def okapi_bm25_similarity(query_vector, document_vector, document_length, avgdl):
    term1 = (K1 + 1) * document_vector
    term2 = K1 * ((1 - B) + B * (document_length / avgdl))
    similarity = query_vector.dot(term1.T) / (query_vector + term2)
    return similarity[0, 0]

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


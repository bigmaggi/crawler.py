from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from sklearn.feature_extraction.text import TfidfVectorizer
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# Elasticsearch configuration
ELASTICSEARCH_HOST = "localhost"
ELASTICSEARCH_PORT = 9200
ELASTICSEARCH_INDEX = "web_indexer"

# Create an Elasticsearch client
client = Elasticsearch(
    hosts=[{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}]
)

# Search the documents in Elasticsearch
def search_documents(client, query, limit=10, num_threads=4):
    # Get all documents from Elasticsearch
    documents = scan(client, index=ELASTICSEARCH_INDEX, query={"query": {"match_all": {}}})

    # Extract the URLs and contents from the documents
    urls = []
    contents = []
    for doc in documents:
        url = doc["_id"]  # Extracting URL from the document's id
        if doc["_source"].get("content") is None:
            continue
        urls.append(url)
        contents.append(doc["_source"]["content"])

    if not urls:
        print("No documents found.")
        return []

    # Apply TF-IDF vectorization to the contents
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(contents)

    # Calculate the cosine similarity between the query vector and document vectors using multiple threads
    query_vector = vectorizer.transform([query])
    cosine_similarities = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in tqdm(range(len(urls)), desc="Calculating similarities"):
            future = executor.submit(cosine_similarity, query_vector, X[i])
            futures.append(future)
        for i, future in enumerate(tqdm(futures, desc="Collecting results", total=len(futures))):
            similarity = future.result()
            cosine_similarities.append((urls[i], similarity))

    # Sort the results by similarity
    sorted_results = sorted(cosine_similarities, key=lambda x: x[1], reverse=True)

    # Return the search results with scores
    return sorted_results[:limit]

# Calculate the cosine similarity between two vectors
def cosine_similarity(v1, v2):
    dot_product = v1.dot(v2.T)
    norm_product = v1.multiply(v1).sum() ** 0.5 * v2.multiply(v2).sum() ** 0.5
    return dot_product / norm_product

def main():
    # Search for a query
    query = input("Enter a search query: ")
    search_results = search_documents(client, query)
    print("Search Results:")
    for result, score in search_results:
        print(f"URL: {result}")
        print(f"Score: {score}")
        print()

if __name__ == "__main__":
    main()


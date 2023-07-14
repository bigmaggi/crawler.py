from elasticsearch import Elasticsearch
import math
from concurrent.futures import ThreadPoolExecutor
from sklearn.feature_extraction.text import TfidfVectorizer
from tqdm import tqdm
from collections import Counter, defaultdict
from joblib import Parallel, delayed

# Elasticsearch configuration
ELASTICSEARCH_HOST = "localhost"
ELASTICSEARCH_PORT = 9200
ELASTICSEARCH_INDEX = "web_indexer"

def bm25(query, document, corpus, k1=1.2, b=0.75):
    """
    Calculates the Okapi BM25 score for a query and document in a corpus.

    Args:
        query (str): The query string.
        document (str): The document string.
        corpus (set): A set of document strings in the corpus.
        k1 (float): A tuning parameter that controls the impact of term frequency.
        b (float): A tuning parameter that controls the impact of document length.

    Returns:
        float: The Okapi BM25 score for the query and document.
    """
    # Tokenize the query and document
    query_tokens = query.split()
    document_tokens = document.split()

    # Calculate the document length
    document_length = len(document_tokens)

    # Calculate the average document length in the corpus
    avg_document_length = sum(len(d.split()) for d in corpus) / len(corpus)

    # Calculate the inverse document frequency for each query term
    idf = defaultdict(lambda: 0)
    for token in query_tokens:
        doc_freq = sum(1 for d in corpus if token in d)
        idf[token] = math.log((len(corpus) - doc_freq + 0.5) / (doc_freq + 0.5))

    # Calculate the term frequency for each query term in the document
    tf = Counter(query_tokens)

    # Calculate the Okapi BM25 score
    score = 0
    for token in query_tokens:
        score += idf[token] * ((tf[token] * (k1 + 1)) / (tf[token] + k1 * (1 - b + b * (document_length / avg_document_length))))

    return score

def search_documents(collection, query, limit=10, num_threads=16):
    documents = collection.find({}, {"url": 1, "content": 1})
    urls = []
    contents = []
    for doc in documents:
        if doc["content"] is None:
            continue
        urls.append(doc["url"])
        contents.append(doc["content"])
    
    if not urls:
        print("No documents found.")
        return []
    
    # Apply TF-IDF vectorization to the contents
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(contents)

    # Calculate the Okapi BM25 score for each document and query pair using multiple threads
    scores = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in tqdm(range(len(urls)), desc="Calculating scores"):
            future = executor.submit(bm25, query, contents[i], contents)
            futures.append(future)
        for i, future in enumerate(tqdm(futures, desc="Collecting results", total=len(futures))):
            score = future.result()
            scores.append({"url": urls[i], "score": score})

    # Sort the results by score
    sorted_results = sorted(scores, key=lambda x: x['score'], reverse=True)

    return sorted_results[:limit]


def main():
    # Connect to MongoDB
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client[MONGODB_DATABASE]
    collection = db[MONGODB_COLLECTION]

    # Search for a query
    query = input("Enter a search query: ")
    search_results = search_documents(collection, query)
    print("Search Results:")
    for result in search_results:
        print(f"URL: {result['url']}")
        print(f"Score: {result['score']}")
        print()


if __name__ == "__main__":
    main()


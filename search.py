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

def search_documents(client, query, limit=10, num_threads=4):
    # Get all documents from Elasticsearch
    documents = client.search(index=ELASTICSEARCH_INDEX, body={"query": {"match_all": {}}}, size=limit)["hits"]["hits"]

    urls = []
    contents = set()
    for doc in documents:
        source = doc["_source"]
        url = source.get("url")
        content = source.get("content")
        if url and content:
            urls.append(url)
            contents.add(content)
    
    if not urls:
        print("No documents found.")
        return []
    
    # Apply TF-IDF vectorization to the contents
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(contents)

    # Calculate the Okapi BM25 score for each document and query pair using multiple threads
    scores = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = Parallel(n_jobs=num_threads, backend="threading")(delayed(bm25)(query, document, contents) for document in tqdm(contents, desc="Calculating scores"))
        scores = list(zip(urls, results))

    # Sort the results by score
    sorted_results = [url for url, score in sorted(scores, key=lambda x: x[1], reverse=True)]

    return sorted_results[:limit]

def main():
    # Connect to Elasticsearch
    client = Elasticsearch([{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}])

    # Search for a query
    query = input("Enter a search query: ")
    search_results = search_documents(client, query)
    print("Search Results:")
    for result in search_results:
        print(f"URL: {result}")
        print()

if __name__ == "__main__":
    main()


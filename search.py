from pymongo import MongoClient
from sklearn.feature_extraction.text import TfidfVectorizer

# MongoDB configuration
MONGODB_CONNECTION_STRING = "mongodb://localhost:27017/"
MONGODB_DATABASE = "web_indexer"
MONGODB_COLLECTION = "documents"

def search_documents(collection, query, limit=10):
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
    tfidf_matrix = vectorizer.fit_transform(contents)
    
    # Compute the cosine similarity between the query and the documents
    query_vector = vectorizer.transform([query])
    cosine_similarities = tfidf_matrix.dot(query_vector.T).toarray().flatten()
    
    # Sort the results by the TF-IDF scores in descending order
    sorted_indices = cosine_similarities.argsort()[::-1]
    sorted_results = [{"url": urls[i], "score": cosine_similarities[i]} for i in sorted_indices]
    
    # Limit the results to the top 'limit' documents
    top_results = sorted_results[:limit]
    
    return top_results


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

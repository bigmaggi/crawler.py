import os
import json
import requests
from datetime import datetime
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from tqdm import tqdm

# Elasticsearch configuration
ELASTICSEARCH_HOST = "localhost"
ELASTICSEARCH_PORT = 9200
ELASTICSEARCH_INDEX = "web_indexer"

# Crawler configuration
BASE_URLS = [
    "https://arxiv.org",
    # Add more base URLs here
    "https://www.mdpi.com",
    "https://core.ac.uk",
    "https://www.sciencedirect.com",
    "https://python.org",
    "https://www.wikipedia.org",
]
SEARCH_URLS = [
    "https://arxiv.org/archive/cs",
    # Add more starting URLs here
    "https://core.ac.uk",
    "https://www.mdpi.com/journal/computers",
    "https://core.ac.uk/search?q=computer+science",
    "https://core.ac.uk/search?q=science",
    "https://www.sciencedirect.com/search?qs=computer%20science",
    "https://www.sciencedirect.com/search?qs=science",
    "https://www.sciencedirect.com/search?qs=computer%20science&show=100",
    "https://www.sciencedirect.com/search?qs=science&show=100",
    "https://www.sciencedirect.com/search?qs=computer%20science&show=100&offset=100",
    "https://www.sciencedirect.com/search?qs=science&show=100&offset=100",
    "https://www.sciencedirect.com/search?qs=computer%20science&show=100&offset=200",
    "https://www.sciencedirect.com/search?qs=science&show=100&offset=200",
    "https://www.python.org/search/?q=&submit=",
    "https://www.wikipedia.org/wiki/Computer_science",
]
VISITED_URLS_FILE = "visited_urls.json"
REVISIT_TIME = 7  # days

# Create an Elasticsearch client
client = Elasticsearch([{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT}])


def crawl(url):
    # Check if the URL has already been visited
    if is_visited(url):
        return

    # Get the HTML content of the URL
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException:
        print(f"Failed to crawl {url}")
        return

    # Parse the HTML content
    soup = BeautifulSoup(response.content, "html.parser")

    # Extract the title and content of the page
    title = soup.title.string.strip() if soup.title else ""
    content = soup.get_text().strip()

    # Index the page in Elasticsearch
    document = {
        "url": url,
        "title": title,
        "content": content
    }
    index_document(document)

    # Mark the URL as visited
    mark_visited(url)


def index_document(document):
    # Index the document in Elasticsearch
    try:
        response = client.index(index=ELASTICSEARCH_INDEX, body=document)
        print(f"Indexed document {response['_id']}")
    except Exception as e:
        print(f"Failed to index document: {e}")


def is_visited(url):
    # Check if the URL has already been visited
    try:
        response = client.get(index=ELASTICSEARCH_INDEX, id=url)
        return response["found"]
    except Exception as e:
        print(f"Failed to check if URL is visited: {e}")
        return False


def mark_visited(url):
    # Mark the URL as visited in Elasticsearch
    visited_url = {
        "url": url,
        "last_visit_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    try:
        response = client.index(index=ELASTICSEARCH_INDEX, id=url, body=visited_url)
        print(f"Marked URL {response['_id']} as visited")
    except Exception as e:
        print(f"Failed to mark URL as visited: {e}")


def load_visited_urls():
    visited_urls = {}

    # Get all visited URLs from Elasticsearch
    try:
        response = client.search(index=ELASTICSEARCH_INDEX, body={"query": {"match_all": {}}})
        for hit in response["hits"]["hits"]:
            visited_urls[hit["_id"]] = hit["_source"]["last_visit_time"]
    except Exception as e:
        print(f"Failed to load visited URLs: {e}")
        return {}

    return visited_urls


def save_visited_urls(visited_urls):
    # Not needed when using Elasticsearch as the database
    pass


def should_revisit(last_visit_time):
    last_visit_datetime = datetime.strptime(last_visit_time, "%Y-%m-%d %H:%M:%S")
    return datetime.now() - last_visit_datetime > timedelta(days=REVISIT_TIME)


def get_base_url(url):
    parsed_url = urlparse(url)
    for base_url in BASE_URLS:
        if parsed_url.netloc == urlparse(base_url).netloc:
            return base_url
    return None


def is_valid_url(url, base_url):
    if url.startswith("mailto:") or url.startswith("tel:") or url.startswith("javascript:") or \
            url.startswith("#") or url.startswith("?") or url.startswith("data:") or url.startswith("irc:") or \
            url.startswith("file:") or url.startswith("ftp:") or url.startswith("sftp:") or url.startswith("ssh:") \
            or not url.startswith(base_url):
        return False
    return True


def main():
    # Load the visited URLs from Elasticsearch
    visited_urls = load_visited_urls()

    # Crawl the web pages
    queue = deque(SEARCH_URLS)
    while queue:
        url = queue.popleft()
        if is_valid_url(url, ""):
            if url not in visited_urls or should_revisit(visited_urls[url]):
                crawl(url)
                visited_urls[url] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                response = requests.get(url)
                soup = BeautifulSoup(response.content, "html.parser")
                for link in soup.find_all("a"):
                    href = link.get("href")
                    absolute_url = urljoin(url, href)
                    base_url = get_base_url(absolute_url)
                    if base_url and is_valid_url(absolute_url, base_url) and absolute_url not in queue:
                        queue.append(absolute_url)
            except requests.exceptions.RequestException:
                print(f"Failed to crawl {url}")

    # Save the visited URLs to Elasticsearch
    for url, last_visit_time in visited_urls.items():
        mark_visited(url)


if __name__ == "__main__":
    main()

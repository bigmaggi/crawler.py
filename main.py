import os
import json
import requests
import asyncio
import aiohttp
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup, SoupStrainer
from elasticsearch.helpers import bulk
from elasticsearch import Elasticsearch
from collections import deque
import elasticsearch
import hashlib
from urllib.parse import urlparse

# Elasticsearch configuration
ELASTICSEARCH_HOST = "localhost"
ELASTICSEARCH_PORT = 9200
ELASTICSEARCH_INDEX = "web_indexer"

SOCIAL_MEDIA_DOMAINS = [
    "facebook.com",
    "twitter.com",
    "linkedin.com",
    "instagram.com",
    "pinterest.com",
    "snapchat.com",
    "tiktok.com",
    "youtube.com",
    # Add more social media domains here
]

# Create an Elasticsearch client
client = Elasticsearch(
    hosts=[f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"]
)

# Define the index settings and mappings
settings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "title": {"type": "text"},
            "content": {"type": "text"},
            "url": {"type": "keyword"},
            "timestamp": {"type": "date"}
        }
    }
}

client.indices.delete(index=ELASTICSEARCH_INDEX, ignore=[400, 404])

# Create the index in Elasticsearch
response = client.indices.create(index=ELASTICSEARCH_INDEX, body=settings)
print(response)

# Crawler configuration
BASE_URLS = [
    "https://arxiv.org",
    # Add more base URLs here
    "https://wikipedia.org",
    "https://www.bbc.com",
    "https://www.cnn.com",
    "https://www.theguardian.com",
    "https://www.nytimes.com",
    "https://www.washingtonpost.com",
    "https://reddit.com",
    "https://tageschau.de",
    "https://www.spiegel.de",
    "https://www.zeit.de",
    "https://www.taz.net",
    "https://www.faz.net",
    "https://www.sueddeutsche.de",
    "https://stackoverflow.com",
    "https://www.github.com",
    "https://www.gitlab.com",
    "https://www.bitbucket.org",
    "https://mathoverflow.net",
    "https://stackexchange.com",
    "https://www.quora.com",
]
SEARCH_URLS = [
    "https://arxiv.org/archive/cs",
    # Add more starting URLs here
    "https://www.wikipedia.org",
    "https://www.bbc.com/news",
    "https://www.cnn.com",
    "https://www.theguardian.com/international",
    "https://www.nytimes.com",
    "https://www.washingtonpost.com",
    "https://reddit.com",
    "https://www.tagesschau.de",
    "https://www.spiegel.de",
    "https://www.zeit.de",
    "https://www.taz.de",
    "https://www.faz.net",
    "https://www.sueddeutsche.de",
    "https://stackoverflow.com",
    "https://www.github.com",
    "https://www.gitlab.com",
    "https://www.bitbucket.org",
    "https://mathoverflow.net",
    "https://stackexchange.com",
    "https://www.quora.com",
]
VISITED_URLS_FILE = "visited_urls.json"
REVISIT_TIME = 7  # days

# Cache configuration
CACHE_EXPIRATION = timedelta(days=1)
URL_CACHE = {}


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
    soup = BeautifulSoup(response.content, "html.parser", parse_only=SoupStrainer(['a', 'title', 'body']))

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

    # Download any PDF, DOC, DOCX, TXT files and index them
    download_files(soup)


def download_files(soup):
    for link in soup.find_all("a"):
        href = link.get("href")
        if href:
            absolute_url = urljoin(BASE_URLS[0], href)
            file_extension = os.path.splitext(href)[1]
            if file_extension.lower() in ['.pdf', '.doc', '.docx', '.txt']:
                try:
                    response = requests.get(absolute_url, stream=True)
                    response.raise_for_status()
                    content_type = response.headers.get("Content-Type", "")
                    if "application/pdf" in content_type:
                        file_extension = ".pdf"
                    elif "application/msword" in content_type:
                        file_extension = ".doc"
                    elif "application/vnd.openxmlformats-officedocument.wordprocessingml.document" in content_type:
                        file_extension = ".docx"
                    elif "text/plain" in content_type:
                        file_extension = ".txt"
                    else:
                        continue
                    file_hash = hashlib.md5(absolute_url.encode()).hexdigest()
                    file_path = f"downloads/{file_hash}{file_extension}"
                    with open(file_path, "wb") as file:
                        for chunk in response.iter_content(chunk_size=8192):
                            file.write(chunk)
                    file_document = {
                        "url": absolute_url,
                        "file_path": file_path,
                        "file_extension": file_extension
                    }
                    index_document(file_document)
                except requests.exceptions.RequestException:
                    print(f"Failed to download {absolute_url}")


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
    except elasticsearch.exceptions.NotFoundError:
        return False
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
    parsed_url = urlparse(url)
    if parsed_url.netloc in SOCIAL_MEDIA_DOMAINS and parsed_url.netloc != "www.reddit.com":
        return False
    if url.startswith("mailto:") or url.startswith("tel:") or url.startswith("javascript:") or \
            url.startswith("#") or url.startswith("?") or url.startswith("data:") or url.startswith("irc:") or \
            url.startswith("file:") or url.startswith("ftp:") or url.startswith("sftp:") or url.startswith("ssh:") \
            or not url.startswith(base_url):
        return False
    return True


def cache_url(url, result):
    URL_CACHE[url] = {
        "result": result,
        "timestamp": datetime.now()
    }


def is_cached(url):
    if url in URL_CACHE:
        timestamp = URL_CACHE[url]["timestamp"]
        if datetime.now() - timestamp < CACHE_EXPIRATION:
            return True
        else:
            del URL_CACHE[url]
    return False


def get_cached_result(url):
    return URL_CACHE[url]["result"]


async def fetch(url, session):
    async with session.get(url) as response:
        return await response.text()


async def async_crawl(url):
    # Check if the URL has already been visited (cached)
    if is_cached(url):
        return get_cached_result(url)

    # Get the HTML content of the URL asynchronously
    async with aiohttp.ClientSession() as session:
        try:
            html = await fetch(url, session)
        except aiohttp.ClientError:
            print(f"Failed to crawl {url}")
            return None

        # Parse the HTML content
        soup = BeautifulSoup(html, "html.parser", parse_only=SoupStrainer(['a', 'title', 'body']))

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

        # Mark the URL as visited (cached)
        cache_url(url, document)

        # Download any PDF, DOC, DOCX, TXT files and index them
        download_files(soup)

        return document


async def main():
    # Load the visited URLs from Elasticsearch
    visited_urls = load_visited_urls()

    # Crawl the web pages using asynchronous requests
    url_queue = deque()

    for url in SEARCH_URLS:
        base_url = get_base_url(url)
        if base_url and is_valid_url(url, base_url):
            url_queue.append(url)

    while url_queue:
        url = url_queue.popleft()

        if url not in visited_urls or should_revisit(visited_urls[url]):
            result = await async_crawl(url)
            visited_urls[url] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            soup = BeautifulSoup(result["content"], "html.parser", parse_only=SoupStrainer('a'))
            for link in soup.find_all("a"):
                href = link.get("href")
                absolute_url = urljoin(url, href)
                if is_valid_url(absolute_url, base_url) and absolute_url not in url_queue:
                    url_queue.append(absolute_url)

    # Save the visited URLs to Elasticsearch
    for url, last_visit_time in visited_urls.items():
        mark_visited(url)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


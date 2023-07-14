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
def create_elasticsearch_client():
    try:
        client = Elasticsearch(
            hosts=[f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"]
        )
        return client
    except Exception as e:
        print(f"Failed to create Elasticsearch client: {e}")
        return None

client = create_elasticsearch_client()

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
    "https://en.wikipedia.org/wiki/Computer_science",
    "https://www.bbc.com/news",
    "https://www.cnn.com",
    "https://edition.cnn.com/world",
    "https://www.theguardian.com/international",
    "https://www.nytimes.com",
    "https://www.nytimes.com/section/world",
    "https://www.washingtonpost.com",
    "https://www.washingtonpost.com/world",
    "https://reddit.com",
    "https://www.reddit.com/r/compsci",
    "https://www.reddit.com/r/programming",
    "https://www.reddit.com/r/learnprogramming",
    "https://www.reddit.com/r/Python",
    "https://www.reddit.com/r/cpp",
    "https://www.reddit.com/r/java",
    "https://www.reddit.com/r/javascript",
    "https://www.reddit.com/r/golang",
    "https://www.reddit.com/r/rust",
    "https://www.reddit.com/r/scala",
    "https://www.reddit.com/r/haskell",
    "https://www.reddit.com/r/programminglanguages",
    "https://www.reddit.com/r/AskComputerScience",
    "https://www.reddit.com/r/computerscience",
    "https://www.reddit.com/r/computersciencehub",
    "https://www.reddit.com/r/computersciencebooks",
    "https://www.reddit.com/r/computersciencehumor",
    "https://www.reddit.com/r/computersciencejobs",
    "https://www.reddit.com/r/ProgrammerHumor",
    "https://www.tagesschau.de",
    "https://www.tagesschau.de/inland",
    "https://www.spiegel.de",
    "https://www.spiegel.de/international",
    "https://www.zeit.de",
    "https://www.zeit.de/politik",
    "https://www.zeit.de/index"
    "https://www.taz.net",
    "https://www.taz.de",
    "https://www.faz.net",
    "https://www.faz.net/aktuell",
    "https://www.sueddeutsche.de",
    "https://www.sueddeutsche.de/politik",
    "https://stackoverflow.com",
    "https://stackoverflow.com/questions/tagged/computer-science",
    "https://www.github.com",
    "https://www.gitlab.com",
    "https://www.bitbucket.org",
    "https://mathoverflow.net",
    "https://stackexchange.com",
    "https://stackexchange.com/sites#traffic",
    "https://stackexchange.com/sites#science",
    "https://stackexchange.com/sites#technology",
    "https://stackexchange.com/sites#lifearts",
    "https://stackexchange.com/sites#culture",
    "https://www.quora.com",
    "https://www.quora.com/topic/Computer-Science",
    "https://www.quora.com/topic/Computer-Science-1",
    "https://www.quora.com/topic/Computer-Science-2",
    "https://www.quora.com/topic/Computer-Science-3",
]
VISITED_URLS_FILE = "visited_urls.json"
REVISIT_TIME = 7  # days

# Cache configuration
CACHE_EXPIRATION = timedelta(days=1)

def get_base_url(url):
    parsed_url = urlparse(url)
    for base_url in BASE_URLS:
        if parsed_url.netloc == urlparse(base_url).netloc:
            return base_url
    return None

def is_valid_url(url, base_url):
    parsed_url = urlparse(url)
    return (is_not_social_media(parsed_url) and
            is_not_special_url(url) and
            is_same_base_url(url, base_url))

def is_not_social_media(parsed_url):
    return parsed_url.netloc not in SOCIAL_MEDIA_DOMAINS or parsed_url.netloc == "www.reddit.com"

def is_not_special_url(url):
    return not (url.startswith("mailto:") or url.startswith("tel:") or url.startswith("javascript:") or 
                url.startswith("#") or url.startswith("?") or url.startswith("data:") or url.startswith("irc:") or 
                url.startswith("file:") or url.startswith("ftp:") or url.startswith("sftp:") or url.startswith("ssh:"))

def is_same_base_url(url, base_url):
    return url.startswith(base_url)

def index_document(document, client):
    try:
        response = client.index(index=ELASTICSEARCH_INDEX, body=document)
        print(f"Indexed document {response['_id']}")
    except Exception as e:
        print(f"Failed to index document: {e}")

def is_visited(url, client):
    try:
        response = client.get(index=ELASTICSEARCH_INDEX, id=url)
        return response["found"]
    except elasticsearch.exceptions.NotFoundError:
        return False
    except Exception as e:
        print(f"Failed to check if URL is visited: {e}")
        return False

def mark_visited(url, client):
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
    try:
        response = client.search(index=ELASTICSEARCH_INDEX, body={"query": {"match_all": {}}})
        for hit in response["hits"]["hits"]:
            visited_urls[hit["_id"]] = hit["_source"]["last_visit_time"]
    except Exception as e:
        print(f"Failed to load visited URLs: {e}")
        return {}

    return visited_urls

def save_visited_urls(visited_urls):
    pass

def should_revisit(last_visit_time):
    last_visit_datetime = datetime.strptime(last_visit_time, "%Y-%m-%d %H:%M:%S")
    return datetime.now() - last_visit_datetime > timedelta(days=REVISIT_TIME)

async def fetch(url, session):
    async with session.get(url) as response:
        return await response.text()

async def async_crawl(url, client):
    if is_visited(url, client):
        return

    async with aiohttp.ClientSession() as session:
        try:
            html = await fetch(url, session)
        except aiohttp.ClientError:
            print(f"Failed to crawl {url}")
            return

    soup = BeautifulSoup(html, "html.parser", parse_only=SoupStrainer(['a', 'title', 'body']))
    title = soup.title.string.strip() if soup.title else ""
    content = soup.get_text().strip()

    document = {
        "url": url,
        "title": title,
        "content": content
    }
    index_document(document, client)
    mark_visited(url, client)
    download_files(soup, client)

    return document

def download_files(soup, client):
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
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    with open(file_path, "wb") as file:
                        for chunk in response.iter_content(chunk_size=8192):
                            file.write(chunk)
                    file_document = {
                        "url": absolute_url,
                        "file_path": file_path,
                        "file_extension": file_extension
                    }
                    index_document(file_document, client)
                except requests.exceptions.RequestException:
                    print(f"Failed to download {absolute_url}")

async def handle_new_url(url, base_url, url_queue, visited_urls):
    if is_valid_url(url, base_url) and url not in visited_urls:
        await url_queue.put(url)

async def main():
    visited_urls = load_visited_urls()
    url_queue = asyncio.Queue()

    for url in SEARCH_URLS:
        base_url = get_base_url(url)
        if base_url and is_valid_url(url, base_url):
            await url_queue.put(url)

    while not url_queue.empty():
        url = await url_queue.get()

        if url not in visited_urls or should_revisit(visited_urls[url]):
            result = await async_crawl(url, client)
            visited_urls[url] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if result is not None:
                soup = BeautifulSoup(result["content"], "html.parser", parse_only=SoupStrainer('a'))
                for link in soup.find_all("a"):
                    href = link.get("href")
                    absolute_url = urljoin(url, href)
                    await handle_new_url(absolute_url, base_url, url_queue, visited_urls)

    for url, last_visit_time in visited_urls.items():
        mark_visited(url, client)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


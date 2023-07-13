import requests
import os
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import json
from pymongo import MongoClient
from collections import deque
from tqdm import tqdm

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
    "https://www.wikipedia.org/wiki/Science",
    "https://www.wikipedia.org/wiki/Computer_science#History",
]
EXCLUDED_URLS = [
    r".*\/help\/.*",
    r".*\/privacy\/.*",
    # Add more wildcard URLs to exclude here
]
DOWNLOAD_DIR = "downloads"
BLACKLIST = ["facebook", "twitter", "instagram", "linkedin", "youtube", "atlassian", "google", "intercom"]
VISITED_URLS_FILE = "visited_urls.json"
REVISIT_TIME = timedelta(days=7)  # Revisit URLs after 7 days

# MongoDB configuration
MONGODB_CONNECTION_STRING = "mongodb://localhost:27017/"
MONGODB_DATABASE = "web_indexer"
MONGODB_COLLECTION = "documents"

def main():
    visited_urls = load_visited_urls()

    # Count the total number of URLs to crawl
    total_urls = len(SEARCH_URLS)
    for base_url in BASE_URLS:
        total_urls += count_urls(base_url)

    # Connect to MongoDB
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client[MONGODB_DATABASE]
    collection = db[MONGODB_COLLECTION]

    with ThreadPoolExecutor(max_workers=16) as executor:
        url_queue = deque(SEARCH_URLS)
        progress_bar = tqdm(total=total_urls)

        completed_urls = set()  # Track processed URLs

        while url_queue:
            url = url_queue.popleft()
            last_visit_time = visited_urls.get(url)

            if last_visit_time and not should_revisit(last_visit_time):
                continue

            base_url = get_base_url(url)
            if base_url is None:
                continue

            try:
                response = fetch_url(url)

                # Parse the HTML content
                soup = BeautifulSoup(response, "html.parser")

                # Find all links on the page
                links = soup.find_all("a", href=True)

                # Add the links to the queue
                for link in links:
                    href = link["href"]
                    if is_valid_url(href, base_url) and href not in completed_urls:
                        url_queue.append(href)
                        completed_urls.add(href)

                # Download the page content
                content = fetch_url(url)

                # Check if the URL is already indexed
                if is_url_indexed(collection, url):
                    print("Skipping already indexed URL:", url)
                    continue

                # Index the document in MongoDB
                index_document(collection, url, content)

                # Update the visited URLs
                visited_urls[url] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            except Exception as e:
                print(f"Error fetching {url}: {e}")

            # Update the progress bar
            progress_bar.update(1)

            # Check if all URLs have been processed
            if len(completed_urls) == total_urls:
                break

    # Save the visited URLs to a file
    save_visited_urls(visited_urls)


def fetch_url(url):
    print("Fetching:", url)
    if url.startswith("ftp://"):
        print(f"Skipping FTP URL: {url}")
        return None

    try:
        response = requests.get(url)
        if isinstance(response.content, str):
            return response.content
        response.raise_for_status()
        return response.text
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"Skipping download for {url}. Error: 403 Forbidden")
        else:
            print(f"Error fetching {url}: {e}")
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error occurred while fetching {url}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching {url}: {e}")

    return None


def process_links(base_url, current_url, response):
    if response is None:
        return []

    links = []
    soup = BeautifulSoup(response, "html.parser")

    for link in soup.find_all("a", href=True):
        href = link.get("href")
        absolute_url = urljoin(current_url, href)

        if is_blacklisted(absolute_url):
            continue

        if href.endswith((".pdf", ".html", ".txt", ".docx", ".doc", ".csv")):
            download_document(base_url, absolute_url)

        try:
            link_response = fetch_url(absolute_url)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"Skipping link {absolute_url}. Error: 404 Not Found")
            else:
                print(f"Error fetching {absolute_url}: {e}")
            continue

        if link_response is None:
            print(f"Skipping link {absolute_url}. Error: No response")
            continue

        if is_excluded_url(absolute_url):
            continue

        links.append(absolute_url)

    return links


def download_document(base_url, url):
    file_name = url.split("/")[-1]
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    if os.path.exists(file_path):
        return

    try:
        response = fetch_url(url)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"Skipping download for {url}. Error: 403 Forbidden")
        elif e.response is None:
            print(f"Skipping download for {url}. Error: No response")
        else:
            print(f"Error fetching {url}: {e}")
        return

    if response is None:
        print(f"Skipping download for {url}. Error: No response")
        return

    with open(file_path, "wb") as file:
        file.write(response.encode("utf-8"))

    print("Downloaded:", url)


def is_blacklisted(url):
    for domain in BLACKLIST:
        if domain in urlparse(url).netloc:
            return True
    return False


def is_excluded_url(url):
    for excluded_url in EXCLUDED_URLS:
        if re.match(excluded_url, url):
            return True
    return False


def load_visited_urls():
    visited_urls = {}

    if os.path.exists(VISITED_URLS_FILE):
        with open(VISITED_URLS_FILE, "r") as file:
            try:
                visited_urls = json.load(file)
            except json.JSONDecodeError:
                print("Invalid JSON data in visited URLs file.")
                return {}

    return visited_urls


def save_visited_urls(visited_urls):
    with open(VISITED_URLS_FILE, "w") as file:
        json.dump(visited_urls, file, indent=4)


def should_revisit(last_visit_time):
    last_visit_datetime = datetime.strptime(last_visit_time, "%Y-%m-%d %H:%M:%S")
    return datetime.now() - last_visit_datetime > REVISIT_TIME


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
            or url.startswith("git:") or url.startswith("svn:") or url.startswith("hg:") or url.startswith("magnet:") \
            or url.startswith("ed2k:") or url.startswith("geo:") or url.startswith("mms:") or url.startswith("rtmp:") \
            or url.startswith("rtsp:") or url.startswith("sms:") or url.startswith("smsto:") or url.startswith("telnet:") \
            or url.startswith("urn:") or url.startswith("webcal:") or url.startswith("wtai:") or url.startswith("xmpp:") \
            or url.startswith("bitcoin:") or url.startswith("ethereum:") or url.startswith("litecoin:") \
            or url.startswith("monero:") or url.startswith("ripple:") or url.startswith("web+") or url.startswith("vcard:"):
        return False

    return urlparse(url).netloc == urlparse(base_url).netloc


def count_urls(url):
    try:
        response = fetch_url(url)
        soup = BeautifulSoup(response, "html.parser")
        links = soup.find_all("a", href=True)
        return len(links)
    except Exception as e:
        print(f"Error counting URLs on {url}: {e}")
        return 0


def index_document(collection, url, content):
    document = {
        "url": url,
        "content": content
    }
    collection.insert_one(document)
    print("Indexed:", url)


def is_url_indexed(collection, url):
    return collection.count_documents({"url": url}) > 0


if __name__ == "__main__":
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
    main()

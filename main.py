from elasticsearch import Elasticsearch
import time
import os
from bs4 import BeautifulSoup
from typing import List, Set
from datetime import timedelta
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser
from tqdm.auto import tqdm
import asyncio
import chardet
import textract
import aiohttp

# replace with your Elasticsearch host and port
ELASTICSEARCH_HOST = "localhost"
ELASTICSEARCH_PORT = 9200

# replace with your Elasticsearch index
ELASTICSEARCH_INDEX = "web_indexer"

# replace with your user agent
ELASTICSEARCH_USER_AGENT = "nightmare_crawler"

# List of social media sites to block
SOCIAL_MEDIA_SITES = [
    "facebook.com",
    "twitter.com",
    "instagram.com",
    "linkedin.com",
    "pinterest.com",
    "tiktok.com",
    "snapchat.com",
    "youtube.com",
    "tumblr.com",
    # Add more social media sites here
]

# List of file extensions to be indexed
SUPPORTED_EXTENSIONS = ["pdf", "txt", "html", "doc", "docx"]

async def create_elasticsearch_client():
    return Elasticsearch([{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT}])

async def fetch_robots_txt(url: str, session):
    try:
        async with session.get(urljoin(url, "/robots.txt")) as response:
            if response.status == 200:
                robots_txt = await response.text()
                robots_parser = RobotFileParser()
                robots_parser.parse([line for line in robots_txt.splitlines() if line.strip()])
                return robots_parser
            else:
                return None
    except Exception as e:
        print(f"Failed to fetch robots.txt from {url}: {e}")
    return None


async def fetch_page(url: str, session, robots_parser):
    try:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"Failed to fetch page {url}: {response.status}")
                return b""
            if not robots_parser.can_fetch(ELASTICSEARCH_USER_AGENT, url):
                print(f"Crawling not allowed on {url}")
                return b""

            content = await response.read()
            return content
    except Exception as e:
        print(f"Failed to fetch page {url}: {e}")
        return b""



async def index_page(client, url: str, content):
    soup = BeautifulSoup(content, "html.parser")

    # Extract the content of the page
    if url.endswith(".pdf"):
        text = textract.process(content).decode("utf-8", errors="ignore")
    else:
        text = soup.get_text()

    # Find other URLs on the page
    urls = set()
    for link in soup.find_all("a"):
        href = link.get("href")
        if href:
            urls.add(urljoin(url, href))

    # Index the page, content, and URLs
    body = {
        "url": url,
        "content": text,
        "urls": list(urls)
    }
    await client.index(index=ELASTICSEARCH_INDEX, document=body)


def calculate_remaining_time(start_time, num_complete, total):
    elapsed_time = time.time() - start_time
    urls_left = total - num_complete
    if num_complete > 0:
        time_per_url = elapsed_time / num_complete
        remaining_time = urls_left * time_per_url
    else:
        remaining_time = 0

    remaining_time = max(remaining_time, 0)  # Ensure remaining time is not negative

    remaining_timedelta = timedelta(seconds=remaining_time)
    return str(remaining_timedelta)


async def crawl(url: str, session, client, robots_parser, visited_urls, pbar, depth=0):
    if url in visited_urls:
        return

    visited_urls.add(url)

    start_time = time.time()

    # Check if the URL matches any of the social media sites
    for site in SOCIAL_MEDIA_SITES:
        if site in url:
            return

    try:
        async with session.get(url) as response:
            content = await response.read()

            if response.status != 200:
                print(f"Failed to fetch page {url}: {response.status}")
                return

            if not robots_parser.can_fetch(ELASTICSEARCH_USER_AGENT, url):
                print(f"Crawling not allowed on {url}")
                return

            await index_page(client, url, content)

            if depth <= 0:
                return

            soup = BeautifulSoup(content, "html.parser")

            # Find other URLs on the page
            urls = set()
            for link in soup.find_all("a"):
                href = link.get("href")
                if href:
                    urls.add(urljoin(url, href))

            for new_url in urls:
                if new_url not in visited_urls:
                    await crawl(new_url, session, client, robots_parser, visited_urls, pbar, depth=depth - 1)

            pbar.set_description(f"Crawled {len(visited_urls)} URLs. Estimated time remaining: {calculate_remaining_time(start_time, len(visited_urls), len(urls))}")
            pbar.update(1)

    except Exception as e:
        print(f"Failed to crawl {url}: {e}")



async def crawl_url(url: str, session):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                title = soup.title.string.strip() if soup.title else ""
                print(f"Crawled {url} - {title}")
            else:
                print(f"Failed to fetch page {url}: {response.status}")
    except Exception as e:
        print(f"Failed to crawl {url}: {e}")



async def main():
    urls = [
        "https://arxiv.org/",
        "https://www.bbc.com/",
        "https://www.cnn.com/",
        "https://www.economist.com/",
        "https://www.forbes.com/",
        "https://www.ft.com/",
        "https://www.theguardian.com/",
        "https://www.independent.co.uk/",
        "https://www.nytimes.com/",
        "https://www.wsj.com/",
        "https://www.washingtonpost.com/",
        "https://www.usatoday.com/",
        "https://www.nbcnews.com/",
        "https://www.cbsnews.com/",
        "https://www.reuters.com/",
        "https://www.bloomberg.com/",
        "https://www.abcnews.go.com/",
        "https://www.npr.org/",
        "https://zeit.de/",
        "https://www.spiegel.de/",
        "https://www.faz.net/",
        "https://www.handelsblatt.com/",
        "https://www.sueddeutsche.de/",
        "https://www.welt.de/",
        "https://www.tagesschau.de/",
        "https://www.wikipedia.org/",
        "https://www.wikipedia.org/wiki/Python_(programming_language)",
        "https://www.wikipedia.org/wiki/Computer_science",
        "https://www.wikipedia.org/wiki/Artificial_intelligence",
        "https://www.wikipedia.org/wiki/Deep_learning",
        "https://www.wikipedia.org/wiki/Machine_learning",
        "https://www.wikipedia.org/wiki/Recurrent_neural_network",
        "https://www.wikipedia.org/wiki/Convolutional_neural_network",
        "https://www.wikipedia.org/wiki/Artificial_neural_network",
        "https://www.wikipedia.org/wiki/Linear_algebra",
        "https://www.wikipedia.org/wiki/Calculus",
        "https://www.stackoverflow.com/",
        "https://www.github.com/",
        "https://www.github.com/elastic/elasticsearch",
        "https://www.gitlab.com/",
        "https://www.gitlab.com/gitlab-org/gitlab",
        "https://www.gitlab.com/gitlab-org/gitlab/-/blob/master/README.md",
        "https://www.python.org/",
        "https://www.python.org/about/",
        "https://www.python.org/about/apps/",
        "https://www.python.org/about/help/",
        "https://www.python.org/about/success/",
        "https://www.python.org/doc/",
        "https://www.python.org/doc/av/",
        "https://quora.com/",
        "https://reddit.com/",
        "https://www.reddit.com/r/learnprogramming/",
        "https://www.reddit.com/r/learnpython/",
        "https://www.reddit.com/r/programming/",
        "https://www.reddit.com/r/python/",
        "https://www.reddit.com/r/technology/",
        "https://www.reddit.com/r/artificial/",
        "https://www.reddit.com/r/machinelearning/",
        "https://www.reddit.com/r/deeplearning/",
        "https://www.reddit.com/r/askprogramming/",
        "https://www.reddit.com/r/askpython/",
        "https://www.reddit.com/r/askcomputerscience/",
        "https://www.reddit.com/r/asktechnology/",
        "https://www.reddit.com/r/askartificial/",
        "https://www.reddit.com/r/ProgramerHumor/",
        "https://news.ycombinator.com/",
        "https://www.dmoz-odp.org/",
        "https://www.dmoz-odp.org/Computers/Programming/Languages/Python/",
        "https://www.dmoz-odp.org/Computers/",
        "https://curlie.org/",
        "https://curlie.org/en",
        "https://curlie.org/de",
        "https://www.wikipedia.org/wiki/9/11",
        "https://www.wikipedia.org/wiki/September_11_attacks",
        "https://www.wikipedia.org/wiki/World_Trade_Center_(1973?2001)",
        "https://www.wikipedia.org/wiki/The_Pentagon",
        "https://www.wikipedia.org/wiki/Germany",
        "https://www.wikipedia.org/wiki/United_States",
        "https://www.wikipedia.org/wiki/United_Kingdom",
        "https://www.wikipedia.org/wiki/France",
        "https://www.wikipedia.org/wiki/Italy",
        "https://www.wikipedia.org/wiki/Spain",
        "https://www.wikipedia.org/wiki/Canada",
        "https://www.wikipedia.org/wiki/India",
        "https://www.wikipedia.org/wiki/China",
        "https://www.wikipedia.org/wiki/Japan",
        "https://www.wikipedia.org/wiki/Russia",
        "https://www.wikipedia.org/wiki/Korea",
    ]  # Add your desired URLs to crawl

    depth = 69420  # Set the crawling depth

		try:
		    await crawl_urls(urls, depth)
		finally:
		    await client.close()


if __name__ == "__main__":
    asyncio.run(main())


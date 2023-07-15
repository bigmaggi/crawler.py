from aiohttp import ClientSession, TCPConnector
from elasticsearch import AsyncElasticsearch
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser
from tqdm import tqdm
import asyncio
import chardet
import time
from bs4 import BeautifulSoup
from typing import List, Set
import PyPDF2
import io
import docx2txt
import textract

# replace with your Elasticsearch host and port
ELASTICSEARCH_HOST = "localhost"
ELASTICSEARCH_PORT = 9200

# replace with your Elasticsearch index
ELASTICSEARCH_INDEX = "web_indexer"

# replace with your user agent
ELASTICSEARCH_USER_AGENT = "nightmare_crawler"

async def create_elasticsearch_client():
    return AsyncElasticsearch(
        [{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}]
    )

async def fetch_robots_txt(url: str, session: ClientSession) -> RobotFileParser:
    try:
        async with session.get(urljoin(url, "/robots.txt")) as response:
            if response.status == 200:
                robots_txt = await response.text()
                robots_parser = RobotFileParser()
                robots_parser.parse(robots_txt.splitlines())
                return robots_parser
    except Exception as e:
        print(f"Failed to fetch robots.txt from {url}: {e}")
    return None

async def fetch_page(url: str, session: ClientSession, robots_parser: RobotFileParser) -> str:
    try:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"Failed to fetch page {url}: {response.status}")
                return ""
            if not robots_parser.can_fetch(ELASTICSEARCH_USER_AGENT, url):
                print(f"Crawling not allowed on {url}")
                return ""

            content = await response.read()
            guess = chardet.detect(content)
            return content.decode(guess.get("encoding", "utf-8"), errors='replace')
    except Exception as e:
        print(f"Failed to fetch page {url}: {e}")
        return ""

async def index_page(client: AsyncElasticsearch, url: str, html: str):
    soup = BeautifulSoup(html, "html.parser")

    # Extract the content of the page
    content = soup.get_text()

    # Find other URLs on the page
    urls = set()
    for link in soup.find_all("a"):
        href = link.get("href")
        if href:
            urls.add(urljoin(url, href))

    # Index the page, content, and URLs
    body = {
        "url": url,
        "content": content,
        "urls": list(urls)
    }
    await client.index(index=ELASTICSEARCH_INDEX, document=body)

def calculate_remaining_time(start_time: float, num_complete: int, total: int) -> str:
    elapsed_time = time.time() - start_time
    urls_left = total - num_complete
    if num_complete > 0:
        time_per_url = elapsed_time / num_complete
        remaining_time = urls_left * time_per_url
    else:
        remaining_time = 0
    minutes, seconds = divmod(remaining_time, 60)
    hours, minutes = divmod(minutes, 60)
    return "%dh %02dm %02ds" % (hours, minutes, seconds)

async def crawl(url: str, session: ClientSession, client: AsyncElasticsearch, robots_parser: RobotFileParser, visited_urls: Set[str], pbar: tqdm, depth: int = 0):
    if url in visited_urls:
        return

    visited_urls.add(url)

    start_time = time.time()  # Define start_time here

    html = await fetch_page(url, session, robots_parser)
    if not html:
        return

    await index_page(client, url, html)

    if depth <= 0:
        return

    soup = BeautifulSoup(html, "html.parser")

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

async def crawl_urls(urls: List[str], depth: int):
    connector = TCPConnector(ssl=False)
		headers = {
    	'User-Agent': ELASTICSEARCH_USER_AGENT
		}

		async with ClientSession(connector=connector, headers=headers) as session:
        client = await create_elasticsearch_client()
        visited_urls = set()

        pbar = tqdm(total=len(urls))

        for url in urls:
            try:
                robots_parser = await fetch_robots_txt(url, session)

                if robots_parser is not None:
                    await crawl(url, session, client, robots_parser, visited_urls, pbar, depth=depth)
            except Exception as e:
                print(f"Failed to crawl {url}: {e}")

        await client.close()
        await session.close()

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
        "https://news.ycombinator.com/"
        "https://www.dmoz-odp.org/"
        "https://www.dmoz-odp.org/Computers/Programming/Languages/Python/",
        "https://www.dmoz-odp.org/Computers/",
        "https://curlie.org/"
        "https://curlie.org/en"
        "https://curlie.org/de"
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
    ]  # Add your desired URLs to crawl

    depth = 1  # Set the crawling depth

    await crawl_urls(urls, depth)

if __name__ == "__main__":
    asyncio.run(main())


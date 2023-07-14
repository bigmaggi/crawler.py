from aiohttp import ClientSession, ClientError, TCPConnector
from elasticsearch import AsyncElasticsearch
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser
import asyncio
import chardet
from bs4 import BeautifulSoup


# replace with your Elasticsearch host and port
ELASTICSEARCH_HOST = "localhost"
ELASTICSEARCH_PORT = 9200

# replace with your Elasticsearch index settings and mappings
ELASTICSEARCH_INDEX = "web_indexer"
ELASTICSEARCH_SETTINGS = {"settings": {}, "mappings": {}}

# replace with your user agent
ELASTICSEARCH_USER_AGENT = "my_crawler"

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
    except ClientError as e:
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

    except ClientError as e:
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

async def crawl(url: str):
    connector = TCPConnector(ssl=False)
    async with ClientSession(connector=connector) as session:
        client = await create_elasticsearch_client()

        try:
            robots_parser = await fetch_robots_txt(url, session)

            if robots_parser is not None:
                html = await fetch_page(url, session, robots_parser)
                # Index the page, content, and URLs
                await index_page(client, url, html)
        finally:
            await client.close()
            await session.close()

async def main():
    url = "https://arxiv.org"  # replace with your URL

    await crawl(url)

if __name__ == "__main__":
    asyncio.run(main())


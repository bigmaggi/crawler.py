import asyncio
from elasticsearch import AsyncElasticsearch
from aiohttp import ClientSession, TCPConnector
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser
from aiohttp.client_exceptions import ServerDisconnectedError

ELASTICSEARCH_HOST = "localhost"
ELASTICSEARCH_PORT = 9200
ELASTICSEARCH_INDEX = "web_indexer"
USER_AGENT = "nightmare_crawler"

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
]

async def create_elasticsearch_client():
    return AsyncElasticsearch([{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT}])

async def fetch_robots_txt(session, url):
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

async def fetch_page(session, url, robots_parser):
    try:
        if robots_parser is not None and not robots_parser.can_fetch(USER_AGENT, url):
            print(f"Crawling not allowed on {url}")
            return None
        async with session.get(url) as response:
            if response.status == 200:
                return await response.text()
            else:
                print(f"Failed to fetch page {url}: {response.status}")
    except Exception as e:
        print(f"Failed to fetch page {url}: {e}")


def extract_links(url, soup):
    return {urljoin(url, link.get('href')) for link in soup.find_all('a') if link.get('href')}

async def index_page(client, url, content):
    soup = BeautifulSoup(content, 'html.parser')
    text = soup.get_text()
    urls = extract_links(url, soup)
    body = {"url": url, "content": text, "urls": list(urls)}
    await client.index(index=ELASTICSEARCH_INDEX, body=body)

async def crawl_url(session, client, url, depth, sem):
    if depth == 0 or any(site in url for site in SOCIAL_MEDIA_SITES):
        return
    async with sem:
        try:
            robots_parser = await fetch_robots_txt(session, url)
            page_content = await fetch_page(session, url, robots_parser)
            if page_content:
                await index_page(client, url, page_content)
                soup = BeautifulSoup(page_content, 'html.parser')
                for link in extract_links(url, soup):
                    await crawl_url(session, client, link, depth-1, sem)
        except ServerDisconnectedError:
            print(f"Server disconnected while crawling {url}.")
        except Exception as e:
            print(f"An error occurred while crawling {url}: {e}")

async def main(urls, depth):
    sem = asyncio.Semaphore(10)  # Limit concurrency
    async with ClientSession(connector=TCPConnector(limit=10)) as session:  # Set limit in the TCPConnector
        client = await create_elasticsearch_client()
        tasks = [crawl_url(session, client, url, depth, sem) for url in urls]
        await asyncio.gather(*tasks)
        await client.close()

if __name__ == "__main__":
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
        # Add your desired URLs to crawl
    ] 
    depth = 69420  # Set the crawling depth
    asyncio.run(main(urls, depth))


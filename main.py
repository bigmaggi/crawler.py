import os
import scrapy
import multiprocessing
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import DropItem
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from PyPDF2 import PdfFileReader
from docx import Document
import tldextract
from langdetect import detect
from scrapy.pipelines.files import FilesPipeline
import requests
import time
from requests.exceptions import TooManyRedirects
from scrapy.pipelines.files import FilesPipeline
from scrapy.exceptions import DropItem
from elasticsearch import Elasticsearch
from tika import parser
from docx import Document
from langdetect import detect
from PyPDF2 import PdfFileReader
from bs4 import BeautifulSoup
from multiprocessing import Pool

ELASTICSEARCH_HOST = 'localhost'
ELASTICSEARCH_PORT = 9200
ELASTICSEARCH_INDEX = 'the_url_index'

BLOCKED_DOMAINS = [
        'facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com',
        'pinterest.com', 'snapchat.com', 'tumblr.com', 'flickr.com',
        'myspace.com', 'meetup.com', 'wechat.com', 'qq.com',
        'tiktok.com', 'whatsapp.com', 'messenger.com', 'viber.com',
        'discord.com', 'telegram.org', 'line.me',
    ]
MAX_DEPTH = 420

class MyFilesPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        return request.url.split("/")[-1]

    def item_completed(self, results, item, info):
        file_paths = [x['path'] for ok, x in results if ok]

        if not file_paths:
            raise DropItem("Item contains no files")

        # Assuming only one file per item
        file_path = file_paths[0]
        content = self.parse_file_content(file_path)

        # Detect the language of the content
        try:
            language = detect(content)
        except:
            language = 'unknown'

        es = Elasticsearch([{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}])
        body = {"url": item["file_urls"][0], "content": content, "language": language}
        es.index(index=ELASTICSEARCH_INDEX, body=body)

        return item

    def parse_file_content(self, file_path):
        if file_path.endswith(".pdf"):
            return self.parse_pdf(file_path)
        elif file_path.endswith(".txt"):
            return self.parse_txt(file_path)
        elif file_path.endswith(".docx"):
            return self.parse_docx(file_path)
        elif file_path.endswith(".html") or file_path.endswith(".htmx"):
            return self.parse_html(file_path)
        else:
            return None

    def parse_html(self, file_path):
        with open(file_path, "r") as file:
            soup = BeautifulSoup(file, 'html.parser')
            return soup.get_text()

    def parse_pdf(self, file_path):
        with open(file_path, "rb") as file:
            reader = PdfFileReader(file)
            return "\n".join(page.extract_text() for page in reader.pages)

    def parse_txt(self, file_path):
        with open(file_path, "r") as file:
            return file.read()

    def parse_docx(self, file_path):
        doc = Document(file_path)
        return "\n".join(paragraph.text for paragraph in doc.paragraphs)


class MySpider(scrapy.Spider):
    name = 'nightmare_spider'
    custom_settings = {
        'ITEM_PIPELINES': {'__main__.MyFilesPipeline': 1},
        'FILES_STORE': 'downloads'
    }

    def __init__(self, start_urls=None, *args, **kwargs):
        super(MySpider, self).__init__(*args, **kwargs)
        self.start_urls = start_urls

    def parse(self, response):
        depth = response.meta.get('depth', 0)
        if depth > MAX_DEPTH:
            return
        content_type = response.headers.get('Content-Type')

        if b'text/html' in content_type:
            yield from self.parse_html(response, depth)
        elif b'application/pdf' in content_type or b'text/plain' in content_type or b'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in content_type:
            yield {'file_urls': [response.url]}

    def parse_html(self, response, depth):
        soup = BeautifulSoup(response.text, 'html.parser')
        text = soup.get_text()
        language = self.detect_language(text)

        urls = {urljoin(response.url, link.get('href')) for link in soup.find_all('a') if link.get('href')}
        body = {"url": response.url, "content": text, "urls": list(urls), "language": language}

        es = Elasticsearch([{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}])
        try:
            es.index(index=ELASTICSEARCH_INDEX, body=body)
        except Exception as e:
            self.log(f"Error indexing document: {e}")

        for url in urls:
            domain = tldextract.extract(url).registered_domain
            if domain not in BLOCKED_DOMAINS:
                yield scrapy.Request(url, callback=self.parse, meta={'depth': depth + 1})

    def detect_language(self, text):
        try:
            return detect(text)
        except:
            return 'unknown'

from bs4 import BeautifulSoup

def extract_links(response):
    if 'text/html' in response.headers['Content-Type']:
        soup = BeautifulSoup(response.content, 'html.parser')
        return [link.get('href') for link in soup.find_all('a') if link.get('href') and link.get('href').startswith('http')]
    else:
        print(f"URL is not HTML: {response.url}")
        return None


def run_spider(urls):
    visited = set()
    errors = {}
    max_errors = 10  # maximum number of errors per url
    retry_after_fail = 5  # seconds to wait after a failed attempt

    # Establish a connection with Elasticsearch
    es = Elasticsearch([{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}])

    while urls:
        url = urls.pop(0)  # get and remove the first url in the list
        if url not in visited and url is not None:
            try:
                response = requests.get(url, timeout=10)
                # handle HTTP error status codes
                if response.status_code == 404:
                    print(f"Page not found: {url}")
                    errors[url] = errors.get(url, 0) + 1
                    if errors[url] > max_errors:
                        print(f"Skipping {url} after {max_errors} failed attempts.")
                        visited.add(url)
                    continue
                elif response.status_code == 403:
                    print(f"Access denied: {url}")
                    errors[url] = errors.get(url, 0) + 1
                    if errors[url] > max_errors:
                        print(f"Skipping {url} after {max_errors} failed attempts.")
                        visited.add(url)
                    continue

                print(f'Crawled {url}')
                visited.add(url)
                links = extract_links(response)
                if links is not None:
                    urls.extend(link for link in links if 'twitter' not in link)  # skip twitter links

                # Index the response content in Elasticsearch
                content = response.text
                language = detect(content)
                body = {"url": url, "content": content, "language": language}
                es.index(index=ELASTICSEARCH_INDEX, body=body)

            except requests.exceptions.RequestException as e:
                print(f"RequestException when trying to get {url}: {e}")
            except Exception as e:
                print(f'Error crawling {url}: {e}')
                print(f"Waiting for {retry_after_fail} seconds before retrying...")
                time.sleep(retry_after_fail)


if __name__ == "__main__":
    all_urls = [
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
      "https://www.scientificamerican.com/podcast/episode/women-smell-better-than-men-09-04-09/",
      "https://www.scientificamerican.com",
      "https://tinygrad.org/",
      "https://www.youtube.com/watch?v=aircAruvnKk",
      "https://phoenixnap.com/kb/check-cpu-usage-load-linux",
      "http://ranprieur.com/essays/dropout.html",
      "https://brokescholar.com/how-to-drop-out-of-college",
      "https://www.baeldung.com/linux/get-cpu-usage",
      "https://www.elastic.co/what-is/vector-search",
      "https://www.algolia.com/blog/ai/what-is-vector-search/" ,
      "https://www.pinecone.io/learn/vector-search-basics/",
      "https://www.infoworld.com/article/3634357/what-is-vector-search-better-search-through-ai.html",
      "https://towardsdatascience.com/text-search-vs-vector-search-better-together-3bd48eb6132a?gi=b01c13254c3a",
      "https://en.wikipedia.org/wiki/Taylor_Swift",
	  "https://comma.ai/",
      "https://www.mayoclinic.org/diseases-conditions/nightmare-disorder/symptoms-causes/syc-20353515",
	  "https://www.mayoclinic.org/",
	  "https://www.sleepfoundation.org/nightmares",
	  "https://www.sleepfoundation.org/",
      "https://www.python.org/downloads/",
      "https://de.wikipedia.org/wiki/Python-3",
      "https://en.wikipedia.org/wiki/Python-3",
      "https://www.digitalocean.com/community/tutorials/how-to-install-and-configure-elasticsearch-on-ubuntu-20-04",
	  "https://www.digitalocean.com/",
	  "https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html",
	  "https://linuxize.com/post/how-to-install-elasticsearch-on-ubuntu-20-04/",
	  "https://phoenixnap.com/kb/install-elasticsearch-ubuntu",
	  "https://learnubuntu.com/install-elasticsearch/",
	  "https://www.taylorswift.com/",
	  "https://www.youtube.com/c/TaylorSwift/videos",
	  "https://www.britannica.com/biography/Taylor-Swift",
      "https://youtube.com/",
      "https://wikipedia.org/",
      "https://amazon.com/",
      "https://openai.com",
      "https://twitch.tv",
      "https://ebay.com ",
      "https://devopedia.org/algorithmic-complexity",
      "https://devopedia.org/",
      "https://devopedia.org/site-map/browse-articles/algorithms",
      "https://www.geeksforgeeks.org/what-is-an-algorithm-definition-types-complexity-examples/",
      "https://www.geeksforgeeks.org/",
      "https://towardsdatascience.com/algorithmic-complexity-101-28b567cc335b",
      "https://towardsdatascience.com/",
      "https://code.visualstudio.com/docs/remote/vscode-server",
      "https://code.visualstudio.com/blogs/2022/07/07/vscode-server",
      "https://code.visualstudio.com/",
      "https://wiki.archlinux.org/title/Redis",
      "https://wiki.archlinux.org/",
      "https://redis.io/docs/getting-started/installation/install-redis-on-linux/",
	  "https://bbc.co.uk",
      "https://cnn.com",
      "https://nytimes.com",
      "https://theguardian.com",
      "https://reuters.com",
	  "https://harvard.edu",
	  "https://mit.edu",
	  "https://stanford.edu",
	  "https://cam.ac.uk",
	  "https://ox.ac.uk"
	  "https://nature.com",
	  "https://sciencemag.org",
	  "https://plos.org",
	  "https://springer.com",
	  "https://elsevier.com",
	  "https://usa.gov",
	  "https://gov.uk",
      "https://europa.eu",
	  "https://ca.gov",
      "https://gov.au",
	  "https://amazon.com",
	  "https://amazon.de",
 	  "https://ebay.com",
	  "https://alibaba.com",
 	  "https://etsy.com",
	  "https://walmart.com",
	  "https://wikipedia.org",
	  "https://imdb.com",
	  "https://archive.org",
	  "https://projectgutenberg.org",
	  "https://techcrunch.com",
	  "https://wired.com",
	  "https://gizmodo.com",
 	  "https://arstechnica.com",
      "https://stackoverflow.com github.com gitlab.com",
      "https://bitbucket.org",
      "https://python.org",
      "https://oracle.com (for Java)",
      "https://microsoft.com (for .NET, C#, etc.)",
      "https://linux.org",
      "https://developer.mozilla.org",
      "https://w3schools.com",
      "https://leetcode.com",
      "https://kaggle.com",
      "https://arxiv.org",
      "https://towardsdatascience.com",
      "https://datasciencecentral.com",
      "https://kdnuggets.com",
      "https://r-bloggers.com",
      "https://tensorflow.org",
      "https://pytorch.org",
      "https://mathoverflow.net",
      "https://math.stackexchange.com",
      "https://stats.stackexchange.com",
      "https://wolfram.com",
      "https://jstatsoft.org",
      "https://physicsworld.com",
      "https://phys.org",
      "https://chemistryworld.com",
      "https://pubs.acs.org",
      "https://nature.com",
      "https://cell.com",
      "https://jamanetwork.com",
      "https://nejm.org",
      "https://pubmed.gov",
      "https://nasa.gov",
      "https://spacex.com"
      "https://astronomy.com",
      "https://skyandtelescope.org",
      "https://nature.com/nclimate/",
      "https://agu.org",
      "https://pubs.geoscienceworld.org",
      "https://adafruit.com",
      "https://sparkfun.com",
      "https://instructables.com",
      "https://makezine.com",
      "https://hackaday.com",
      "https://coursera.org",
      "https://edx.org",
      "https://ocw.mit.edu",
      "https://khanacademy.org",
      "https://arxiv.org",
      "https://biorxiv.org",
      "https://chemrxiv.org",
	  "https://eventim.de",
	  "https://theweblist.net/",
      "https://www.winehq.org/"  ,
      "https://stackoverflow.com/questions/33675945/optimal-way-to-set-up-elk-stack-on-three-servers",
      "https://www.elastic.co/guide/en/elasticsearch/reference/current/scalability.html",
      "https://stackoverflow.com/questions/71729223/how-to-connect-to-multiple-servers-in-spring-data-elasticsearch",
      "https://xyzcoder.github.io/2020/07/22/how-to-deploy-an-elastic-search-cluster-consisting-of-multiple-hosts-using-es-docker-image.html",
      "https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-node.html",
      "https://xyzcoder.github.io/index.html",
      "https://chrome.google.com/webstore/detail/noscript/doojmbjmlfjjnbmnoijecmcbfeoakpjm",
      "https://microsoftedge.microsoft.com/addons/detail/noscript/debdhlbmgmkkfjpcglcbjadbhhekgfjh",
      "https://addons.mozilla.org/en-US/firefox/addon/noscript/",
      "https://noscript.net/",
      "https://www.howtogeek.com/138865/htg-explains-should-you-disable-javascript/",
      "https://support.google.com/chrome/answer/114662?hl=en&co=GENIE.Platform=Desktop",
      "https://answers.microsoft.com/en-us/microsoftedge/forum/all/microsoft-edge-clipboard-access-prompt/71ba6f2c-e1f2-45fd-a57c-d5cddc3488f3",
      "https://windowsreport.com/allow-webpage-to-access-clipboard/",
      "https://www.reddit.com/r/sysadmin/comments/t4jsqh/microsoft_edge_site_permissions_how_the_heck_do/",
      "https://web.dev/async-clipboard/",
      "https://www.w3.org/TR/clipboard-apis/",
      "https://www.w3.org/TR/clipboard-apis/#async-clipboard-api",
      "https://www.w3.org/TR/",
      "https://www.w3.org/",
      "https://www.reddit.com/",
      "https://www.medium.com/",
      "https://arxiv.org/abs/2307.09042v1",
      "https://emotional-intelligence.github.io/",
      "https://www.biorxiv.org/content/10.1101/2023.07.17.549421v1",
      "https://aclanthology.org/2021.findings-acl.379.pdf",
      "https://www.brookings.edu/articles/exploring-the-impact-of-language-models/",
    ]

    # split the urls into chunks
    urls_chunks = [all_urls[i::48] for i in range(48)]

    with Pool(processes=48) as pool:
        pool.map(run_spider, urls_chunks)

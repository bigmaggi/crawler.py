import os
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.pipelines.files import FilesPipeline
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from PyPDF2 import PdfFileReader
from docx import Document

ELASTICSEARCH_HOST = "localhost"
ELASTICSEARCH_PORT = 9200
ELASTICSEARCH_INDEX = "web_indexer"

class MyFilesPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        return request.url.split("/")[-1]

    def item_completed(self, results, item, info):
        file_paths = [x['path'] for ok, x in results if ok]

        if not file_paths:
            raise DropItem("Item contains no files")

        # Assuming only one file per item
        file_path = file_paths[0]

        if file_path.endswith(".pdf"):
            content = self.parse_pdf(file_path)
        elif file_path.endswith(".txt"):
            content = self.parse_txt(file_path)
        elif file_path.endswith(".docx"):
            content = self.parse_docx(file_path)
        else:
            return item

        es = Elasticsearch([{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT}])
        body = {"url": item["file_urls"][0], "content": content}
        es.index(index=ELASTICSEARCH_INDEX, body=body)

        return item

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

class MySpider(scrapy.Spider, start_urls):
    name = 'nightmare_spider'

    custom_settings = {
        'ITEM_PIPELINES': {'__main__.MyFilesPipeline': 1},
        'FILES_STORE': 'downloads'
    }

    start_urls = start_urls

    def parse(self, response):
        content_type = response.headers.get('Content-Type')

        if b'text/html' in content_type:
            yield from self.parse_html(response)
        elif b'application/pdf' in content_type or b'text/plain' in content_type or b'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in content_type:
            yield {'file_urls': [response.url]}

    def parse_html(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        text = soup.get_text()
        urls = {urljoin(response.url, link.get('href')) for link in soup.find_all('a') if link.get('href')}
        body = {"url": response.url, "content": text, "urls": list(urls)}

        es = Elasticsearch([{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT}])
        es.index(index=ELASTICSEARCH_INDEX, body=body)

        for url in urls:
            yield scrapy.Request(url, callback=self.parse)

process = CrawlerProcess(settings={
    "FEEDS": {
        "items.json": {"format": "json"},
    },
})

process.crawl(MySpider)
process.start()  # the script will block here until the crawling is finished


import multiprocessing
from scrapy.crawler import CrawlerProcess
from MySpider import MySpider

def run_spider(urls):
    process = CrawlerProcess(settings={
        "FEEDS": {
            "items.json": {"format": "json"},
        },
    })
    process.crawl(MySpider, start_urls=urls)
    process.start()  # Script will block here until crawling is finished

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
# Add your desired URLs to crawl

    ]

    # Split the URLs into 16 equal-sized chunks
    urls_chunks = [all_urls[i::16] for i in range(16)]

    with multiprocessing.Pool(processes=16) as pool:
        pool.map(run_spider, urls_chunks)


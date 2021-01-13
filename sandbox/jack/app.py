import faust
import requests
import boto3
import json
from mode.utils.aiter import aiter

from helpers import _tag_visible, _text_from_html, _urls_from_html

BUCKET = 'faust-sandbox'
USER = 'jack'
MAX_DEPTH = 2
PARTITIONS = 4

class URL(faust.Record, serializer="json"):
    domain: str
    url: str
    depth: int

class SuccessPage(faust.Record, serializer="json"):
    url: str
    html: str
    status_code: int
    domain: str
    depth: int

app = faust.App(
    "scraper",
    broker="kafka://localhost:9092",
    topic_partitions=2,
    store="rocksdb://"
)

urls = app.topic("urls", value_type=URL)
successful_urls = app.topic("successful_urls", value_type=SuccessPage)

@app.agent(urls)
async def process_url(urls):
    """Processes the urls into successful pages
    """
    async for url in urls:
        r = requests.get(url.url)
        if r.status_code == 200:
            print(f'Successful URL: {url.url}')
            page =  SuccessPage(
                    url=url.url,
                    html=r.content.decode('utf-8'),
                    status_code=r.status_code,
                    depth=url.depth,
                    domain=url.domain
                )
        await process_html.send(value=page)


def make_key(url):
    """ Amends url so that it can be used as an object key.
    """
    return f'{USER}/{url.replace("/","|")}.json'


@app.agent(successful_urls)
async def process_html(pages):
    """ Saves to S3, extracts URLS from page
    """
    async for page in pages:
        text = _text_from_html(page.html)
        s3 = boto3.resource('s3')
        object = s3.Object(BUCKET, make_key(page.url))
        object.put(Body=json.dumps(text))
        depth = int(page.depth)
        if depth == MAX_DEPTH: # prevents continuation once max depth reached
            continue
        for next_url in _urls_from_html(page.html):
            # Filters for next URLs
            if page.domain not in next_url:
                continue
            if "@" in next_url:
                continue
            if not next_url.startswith('http://'):
                continue
            urls=URL(url=next_url,
                    domain=page.domain,
                    depth=depth+1)
            await process_url.send(value=urls)
import faust
import requests
import aioboto3
import json

from helpers import _tag_visible, _text_from_html, _urls_from_html

BUCKET = 'faust-sandbox'
USER = 'jack'
MAX_DEPTH = 2
PARTITIONS = 4

class URL(faust.Record, serializer="json"):
    name: str
    url: str

class SuccessPage(faust.Record, serializer="json"):
    url: str
    html: str
    status_code: int


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
    """Map URL to {status code of requests} and {rendered text}"""
    async for url in urls:
        r = requests.get(url.url)
        if r.status_code == 200:
            print(f'Successful URL: {url.url}')
            page =  SuccessPage(
                    url=url.url,
                    html=r.content.decode('utf-8'),
                    status_code=r.status_code,
                )
        await process_html.send(value=page)

async def already_done_url(url):
    """
    Dummy function: need to work out how to check if
    RenderedPage already exists for this URL
    """
    async with aioboto3.resource("s3") as s3:
        bucket = await s3.Bucket(BUCKET)
        objs = bucket.objects.filter(Prefix=make_key(url))
        async for _ in objs:
            return True
    return False


def make_key(url):
    return f'{USER}/{url.replace("/","|")}.json'


@app.agent(successful_urls)
async def process_html(pages):
    """Extract text and yield any URLs found on the page"""
    async for page in pages:
        # Extract text and save to S3
        text = _text_from_html(page.html)
        async with aioboto3.resource("s3") as s3:
            object = await s3.Object(BUCKET, make_key(page.url))
            await object.put(Body=json.dumps(text))
        # Don't go beyond MAX DEPTH
        depth = int(page.depth)
        if depth == MAX_DEPTH:
            continue
        # Yield any URLs found
        for next_url in _urls_from_html(page.html):
            # Don't go to external URLs
            if page.domain not in next_url:
                continue
            # Don't do email addresses
            if "@" in next_url:
                continue
            # Require a scheme
            if not next_url.startswith('http://'):
                continue
            # Don't do files
            # if any(next_url.endswith(f'.{suffix}') for suffix in ['pdf', 'jpg', 'jpeg',\
            #                     'png', 'doc', 'docx',\
            #                     'txt', 'csv', 'xls', 'xlsx']:
            #                     continue
            # # Don't redo any done pages
            if await already_done_url(next_url):
                continue
            # Yield away
            urls=URL(url=next_url,
                    domain=page.domain,
                    depth=depth+1)
            await process_url.send(value=URL)
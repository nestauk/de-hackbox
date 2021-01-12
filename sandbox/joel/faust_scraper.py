import json
from typing import Optional

import aioboto3
import faust
import requests
from mode.utils.aiter import aiter

from utils import _tag_visible, _text_from_html, _urls_from_html

# These should go in a config
BUCKET = 'faust-sandbox'
USER = 'joel'
MAX_DEPTH = 2
PARTITIONS = 4

# Global app declaration
app = faust.App('faust_scraper',
                broker='kafka://localhost:9092',
                store='rocksdb://',
                topic_partitions=PARTITIONS)


# "Schema"
class UrlEvent(faust.Record, serializer="json"):
    url: str
    domain: str
    depth: int
    parent: Optional[str]


class RenderedPage(faust.Record, serializer="json"):
    url: str
    domain: str
    depth: int
    html: str


# Topics, tables and predefined streams
url_events = app.topic('url_events',
                       value_type=UrlEvent)
rendered_pages = app.topic('rendered_pages',
                           value_type=RenderedPage)
status_code_counts = app.Table('status_code_counts',
                               default=int,
                               partitions=PARTITIONS)


# Agents
@app.agent(url_events)
async def process_url(events):
    """Map URL to {status code of requests} and {rendered text}"""
    async for event in events:
        # Try to hit the page
        r = requests.get(event.url)
        status_code_counts[r.status_code] += 1
        # If successful, send the page off to be parsed
        if r.status_code == 200:
            page = RenderedPage(url=event.url,
                                domain=event.domain,
                                html=r.content.decode(r.encoding),
                                depth=event.depth)
            await process_text.send(value=page)


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


@ app.agent(rendered_pages)
async def process_text(pages):
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
            if not next_urls.startswith('http://'):
                continue
            # Don't do files
            if any(next_url.endswith(f'.{suffix}')
                   for suffix in ['pdf', 'jpg', 'jpeg',
                                  'png', 'doc', 'docx',
                                  'txt', 'csv', 'xls', 'xlsx']:
                   continue
            # Don't redo any done pages
            if await already_done_url(next_url):
                continue
            # Yield away
            url_event=UrlEvent(url=next_url,
                                 domain=page.domain,
                                 parent=page.url,
                                 depth=depth+1)
            await process_url.send(value=url_event)

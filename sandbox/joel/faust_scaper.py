import json
from typing import Optional

import aioboto3
import faust
import requests

from utils import _tag_visible, _text_from_html

MAX_DEPTH = 2
PARTITIONS = 4

app = faust.App('faust_scraper',
                broker='kafka://localhost:9092',
                store='rocksdb://',
                topic_partitions=PARTITIONS)


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
    status_code: int


url_events = app.topic('url_events',
                       value_type=UrlEvent)
rendered_pages = app.topic('rendered_pages',
                           value_type=RenderedPage)
status_code_counts = app.Table('status_code_counts',
                               default=int,
                               partitions=PARTITIONS)


@app.agent(url_events)
async def process_url(events):
    """Map URL to {status code of requests} and {rendered text}"""
    async for event in events:
        r = requests.get(event.url)
        status_code_counts[r.status_code] += 1
        if r.status_code == 200:
            page = RenderedPage(url=event.url,
                                domain=event.domain,
                                html=r.content.decode('utf-8'),
                                status_code=r.status_code,
                                depth=event.depth)
            await process_text.send(value=page)


@app.agent(rendered_pages)
async def process_text(pages):
    """"""
    async for page in pages:
        text = _text_from_html(page.html)
        async with aioboto3.resource("s3") as s3:
            name = f'joel/{page.url.replace("/","|")}.json'
            object = await s3.Object('faust-sandbox', name)
            await object.put(Body=json.dumps(text))
        if page.depth == MAX_DEPTH:
            return

        # async for next_url in _urls_from_html(text):
        #     # TODO: IF URL NOT IN QUEUE ALREADY
        #     UrlEvent.send(url=next_url,
        #                   domain=domain,
        # parent=url,

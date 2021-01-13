from typing import Optional

import faust
import requests
from mode.utils.aiter import aiter

from config import config
from utils import (already_done_url, put_to_s3, skip_this_url, text_from_html,
                   urls_from_html)

# Global app declaration
app = faust.App(**config['app'])


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
url_events = app.topic('url_events', value_type=UrlEvent)
rendered_pages = app.topic('rendered_pages', value_type=RenderedPage)
status_code_counts = app.Table('status_code_counts', default=int,
                               partitions=config['app']['topic_partitions'])


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


@app.agent(rendered_pages)
async def process_text(pages):
    """Extract text and yield any URLs found on the page"""
    async for page in pages:
        # Extract text and save to S3
        text = text_from_html(page.html)
        await put_to_s3(page.url, text)
        # Don't go beyond max_depth
        depth = int(page.depth)
        if depth == config['max_depth']:
            continue
        # Yield any URLs found
        for next_url in urls_from_html(page.html):
            # Filter URLs
            if skip_this_url(next_url):
                continue
            if await already_done_url(next_url):
                continue
            # If is external
            if (domain not in next_url):
                # Exception is a path from the current page
                if next_url.startswith('/'):
                    next_url = f'{url}{next_url}'
                # Otherwise skip it
                else:
                    continue
            # Yield is this is a good URL that hasn't been processed
            url_event = UrlEvent(url=next_url,
                                 domain=page.domain,
                                 parent=page.url,
                                 depth=depth+1)
            await process_url.send(value=url_event)

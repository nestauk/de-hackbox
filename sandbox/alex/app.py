import logging
from typing import Tuple, Union
from urllib3.util.url import parse_url

import aioboto3
import aiohttp
import faust
from bs4 import BeautifulSoup

from schema import UniversityURL, Response, RenderedText
from utils import in_s3, extract_internal_links

MAX_DEPTH = 2
BUCKET = "faust-sandbox"
USER = "alex"


app = faust.App(
    "scraper",
    broker="kafka://localhost:9092",
    topic_partitions=4,
    store="rocksdb://",
    producer_max_request_size=10_000_000,  # TODO: handle failure case
)

# TOPICS
# ======

# Input queue
university_urls = app.topic("university_urls", value_type=UniversityURL, internal=True)

# URL's that don't exist
bad_urls = app.topic("bad_urls", value_type=UniversityURL, internal=True)

# URL's with status 200
successful_response = app.topic(
    "successful_response", value_type=Response, internal=True
)
# URL's with bad status
unsuccessful_response = app.topic(
    "unsuccessful_response", value_type=Response, internal=True
)

# Scraped rendered text
rendered_text = app.topic("rendered_text", value_type=RenderedText)


# AGENTS
# ======


@app.agent(successful_response)
async def log_success(messages):
    """Logs URL's with 200 status."""
    async for message in messages:
        logging.info(f"SUCCESS: {message.request} has status code {message.status}")


@app.agent(unsuccessful_response)
async def log_failure(messages):
    """Logs URL's with bad status codes."""
    async for message in messages:
        logging.warning(f"FAILURE: {message.request} has status code {message.status}")


@app.agent(bad_urls)
async def log_bad_url(messages):
    """Logs URL's which don't exist."""
    async for message in messages:
        logging.error(f"URL doesn't exist: {message.url}")


@app.agent(university_urls)
async def ping_url(requests):
    """Request URL and dispatch to other queue based on status code."""
    session = aiohttp.ClientSession()
    async for request in requests:
        # TODO: IF WE DECIDE TO UP `MAX_DEPTH` then how can we re-trigger the
        # collection for a site?
        if request.depth <= MAX_DEPTH and not await repeat(request):
            logging.info(f"{request.url} enqueued by {request.parent_url}")
            channel, response = await process_url(session, request)
            await channel.send(value=response)


async def repeat(request: UniversityURL) -> bool:
    """Determines whether `request.url` has already been persisted to S3.

    WARNING: `request.url` may have been scraped but not yet persisted to S3.
    """
    key = persist_key(request.url)
    return await in_s3(BUCKET, key)


async def process_url(
    session, request: UniversityURL
) -> Tuple[faust.TopicT, Union[Response, UniversityURL]]:
    """Process request returning a record and the topic to send to."""
    try:
        async with session.get(request.url) as response:
            response.raise_for_status()
            # 200 OK
            return (
                successful_response,
                Response(
                    request=request,
                    status=response.status,
                    reason=response.reason,
                    response_text=await response.text(),
                ),
            )
    except aiohttp.client_exceptions.ClientConnectionError:
        # URL doesn't exist
        return (bad_urls, request)
    except aiohttp.client_exceptions.ClientResponseError:
        # Bad HTTP status
        return (
            unsuccessful_response,
            Response(
                request=request,
                status=response.status,
                reason=response.reason,
            ),
        )


@app.agent(successful_response)
async def soup_kitchen(responses):
    async for response in responses:
        if response.request.depth > MAX_DEPTH:
            continue

        soup = BeautifulSoup(response.response_text, features="lxml")

        if response.request.depth < MAX_DEPTH:
            # Find internal links
            host = parse_url(response.request.parent_url or response.request.url)
            links = extract_internal_links(soup, host=host)
            # Send to queue
            [
                await university_urls.send(
                    value=UniversityURL(
                        name=response.request.name,
                        url=link.url,
                        parent_url=response.request.url,
                        depth=response.request.depth + 1,
                    )
                )
                for link in links
            ]

        # Export soup
        await rendered_text.send(
            value=RenderedText(url=response.request.url, text=soup.text)
        )


@app.agent(rendered_text)
async def s3_persist(texts):
    async for text in texts:
        key = persist_key(text.url)
        logging.info(f"PERSISTING: {key}")

        async with aioboto3.resource("s3") as s3:
            obj = await s3.Object("faust-sandbox", key)
            await obj.put(Body=text.text.encode())


def persist_key(url: str) -> str:
    """S3 Key to persist `url` at."""
    url_ = parse_url(url)
    return f"{USER}/{url_.host.replace('/', '|')}/{url.replace('/', '|')}"

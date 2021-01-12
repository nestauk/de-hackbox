import logging
from typing import Optional, Tuple, Union

import aiohttp
import faust


class UniversityURL(faust.Record, serializer="json"):
    # University name
    name: str
    # URL to scrape:
    url: str
    # URL that enqueued this URL, None if domain root:
    parent_url: Optional[str]


class Response(faust.Record, serializer="json"):
    request: UniversityURL
    reason: str
    status: int
    response_text: Optional[str]


app = faust.App(
    "scraper", broker="kafka://localhost:9092", topic_partitions=4, store="rocksdb://"
)
university_urls = app.topic("university_urls", value_type=UniversityURL, internal=False)

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
        logging.debug(f"{request.url} enqueued by {request.parent_url}")
        channel, response = await process_url(session, request)
        await channel.send(value=response)


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

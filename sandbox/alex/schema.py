from typing import Optional

import faust


class UniversityURL(faust.Record, serializer="json"):
    # University name
    name: str
    # URL to scrape:
    url: str
    # URL that enqueued this URL, None if domain root:
    parent_url: Optional[str]
    # Depth of scraper
    depth: int = 0


class Response(faust.Record, serializer="json"):
    request: UniversityURL
    reason: str
    status: int
    response_text: Optional[str]


class RenderedText(faust.Record, serializer="json"):
    url: str
    text: str

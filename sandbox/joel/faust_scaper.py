import faust
from utils import _tag_visible, _text_from_html

MAX_DEPTH = 2

app = faust.App('faust_scraper',
                broker='kafka://localhost:9092',
                store='rocksdb://',
                topic_partitions=4)


class UrlEvent(faust.Record, serializer="json"):
    url: str
    domain: str
    depth: int
    parent: str


class RenderedPage(faust.Record, serializer="json"):
    url: str
    domain: str
    depth: int
    html: str
    status: int


url_events = app.topic('url_events',
                       value_type=UrlEvent)
rendered_pages = app.topic('rendered_pages',
                           value_type=RenderedPage)
status_code_counts = app.Table('status_code_counts',
                               default=int)


@ app.agent(url_events)
async def process_url(events):  # url, domain):
    """Map URL to {status code of requests} and {rendered text}"""
    async for event in events:
        print('process_url::', "processing", event.url)
        r = requests.get(event.url)
        print('process_url::', "status", r.status_code)
        if r.status_code == 200:
            print('process_url::', "sending")
            RenderedText.send(url=event.url,
                              domain=event.domain,
                              text=r.content.decode('utf-8'),
                              status_code=r.status_code,
                              depth=event.depth)
    StatusCodeTable[r.status_code] += 1


@app.agent(rendered_pages)
async def process_text(pages):
    """"""
    async for page in pages:
        print('process_text::', 'processing', page.url)
        text = _text_from_html(page.text)
        print('process_text::', text)
        # TODO: boto3.dump(text)
        if depth == MAX_DEPTH:
            return
        # async for next_url in _urls_from_html(text):
        #     # TODO: IF URL NOT IN QUEUE ALREADY
        #     UrlEvent.send(url=next_url,
        #                   domain=domain,

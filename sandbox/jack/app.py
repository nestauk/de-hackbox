import faust
import requests


class URL(faust.Record, serializer="json"):
    name: str
    url: str

class SuccessPage(faust.Record, serializer="json"):
    url: str
    html: str
    status_code: int

class FailPage(faust.Record, serializer="json"):
    url: str
    html: str
    status_code: int

app = faust.App(
    "scraper", broker="kafka://localhost:9092", topic_partitions=2, store="rocksdb://"
)

urls = app.topic("urls", value_type=URL)
successful_urls = app.topic("successful_urls", value_type=SuccessPage)
unsuccessful_urls = app.topic("unsuccessful_urls", value_type=FailPage)

@app.agent(urls)
async def process_url(urls):
    """Map URL to {status code of requests} and {rendered text}"""
    async for url in urls:
        r = requests.get(url.url)
        if r.status_code == 200:
            print(f'Successful URL: {url.url}')
            return (
                successful_urls,
                SuccessPage(
                    url=url.url,
                    html=r.content.decode('utf-8'),
                    status_code=r.status_code,
                )
            )
        if r.status_code != 200:
            print(f'Unsuccessful URL: {url.url}')
            return (
                unsuccessful_urls,
                FailPage(
                    url=url.url,
                    status_code=r.status_code,
                )
            )

# TODO agent to process the html

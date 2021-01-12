import faust

class URL(faust.Record, serializer="json"):
    name: str
    url: str

class Response(faust.Record, serializer="json"):
    request: URL
    status: int

app = faust.App(
    "scraper", broker="kafka://localhost:9092", topic_partitions=2, store="rocksdb://"
)
urls = app.topic("urls", value_type=URL, internal=False)
nonexistent_urls = app.topic("nonexistent_urls", value_type=URL, internal=True)
successful_urls = app.topic("successful_urls", value_type=Response, internal=True)
unsuccessful_urls = app.topic("unsuccessful_urls", value_type=Response, internal=True)

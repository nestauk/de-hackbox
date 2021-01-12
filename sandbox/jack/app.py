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

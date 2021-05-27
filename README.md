# faust-scraper
[Experimental] 

In Jan 2021 we did a mini hack-week to play around with Faust and Kafka. In the end, we decided to try to use Faust/Kafka for crawling large websites by recursively queuing internal links.

Our various attempts can be found the `sandbox` directory:

* [Joel's attempt](sandbox/joel/)

# Tutorials and documentation

I first recommend watching the first four videos in [Confluent's Kakfa series](https://www.youtube.com/watch?v=Z3JKCLG3VP4&feature=emb_logo), which I'd also recommend speeding up to 1.5x.

For practical implementation in Python, we followed the [faust documentation](https://faust.readthedocs.io/en/latest/index.html) and tutorials therein.

To start Kafka and Zooperkeeper, follow the first two steps on the [Kakfa Quickstart page](https://kafka.apache.org/quickstart#quickstart_download).

# Notes to self

## Deleting faust table changelogs

From the Kafka server directory:

```bash
bin/kafka-topics.sh --delete --topic <faust-app-name>-<table_name>-changelog --bootstrap-servelocalhost:9092
```

## Nuclear option for a clean workspace

Stop the Faust server, Kafka server and then Zoopkeeper server (in that order!). Then do:

```bash
rm -rf /tmp/kafka-logs /tmp/zookeeper
```

and also

```bash
rm -r <faust-app-name>-data/
```

this will give you a clean slate to run from.

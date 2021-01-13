# faust-scraper
[Experimental] Using faust/kafka for webscraping by recursively queuing internal links



## Notes to self

### Deleting faust table changelogs

From the Kafka server directory:

```bash
bin/kafka-topics.sh --delete --topic <faust-app-name>-<table_name>-changelog --bootstrap-servelocalhost:9092
```

### Nuclear option for a clean workspace

Stop the Faust server, Kafka server and then Zoopkeeper server (in that order!). Then do:

```bash
rm -rf /tmp/kafka-logs /tmp/zookeeper
```

and also

```bash
rm -r <faust-app-name>-data/
```

this will give you a clean slate to run from.

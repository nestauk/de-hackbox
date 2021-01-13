faust-scraper
=============

Crawling a university website using Faust and Kafka, just to see how it works.

Before getting started
----------------------

Set up the Zookeeper and Kafka servers, as described on the top README of this repo. 

Make sure that you have S3 set up, so that you can use `boto3`. The details of buckets and app settings can be found in `config.yaml`.

Finally, make sure you have installed the requirements as listed in `requirements.txt`.

Running the pipeline
--------------------

You can run start the Faust server from this directory with:

```bash
bash bin/start_faust_worker.sh
```

and then trigger the scraper to run on the University of Manchester's website using:

```bash
bash bin/trigger_pipeline.sh
```

At time of writing, going to a maximum depth of two pages yields 26 results on S3 for me. To re-trigger the pipeline, change or clear your bucket, since S3 is currently used for checking whether a URL has been processed already.

from kafka import KafkaAdminClient

client = KafkaAdminClient()

client.delete_topics(
    [
        "university_urls",
        "successful_response",
        "unsuccessful_response",
        "bad_urls",
        "scraper-__assignor-__leader",
        "rendered_text",
    ]
)

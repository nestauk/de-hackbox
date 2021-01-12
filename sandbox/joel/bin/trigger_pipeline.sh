#!/usr/bin/env bash
faust -A faust_scraper send url_events '{"url": "http://manchester.ac.uk/", "domain": "manchester.ac.uk", "depth": "1"}'

#!/usr/bin/env bash
faust -A faust_scaper send url_events '{"url": "http://manchester.ac.uk/", "domain": "manchester.ac.uk", "depth": "1"}'

#!/bin/sh

cd "$(dirname "$0")"
faust -W ../ -A app send @ping_url '{"url": "https://warwick.ac.uk", "parent_url": "2", "name": "University of life"}'

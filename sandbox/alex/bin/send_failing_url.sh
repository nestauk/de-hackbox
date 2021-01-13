#!/bin/sh

cd "$(dirname "$0")"
faust -W ../ -A app send @ping_url '{"url": "https://warwick.ac.uk/this_page_shouldnt_exist", "name": "University of life"}'


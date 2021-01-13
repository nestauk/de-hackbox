#!/bin/sh

cd "$(dirname "$0")"
faust -W ../ -A app send @ping_url '{"url": "https://warwickasdjlkfasdf.ac.uk", "name": "University of life"}'


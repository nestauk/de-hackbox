#!/bin/sh

python -m faust -A app worker -l info --web-port=6066

import json

import aioboto3
import requests
from bs4 import BeautifulSoup
from bs4.element import Comment

from config import config

INVISIBLE_TAGS = ['style', 'script', 'head',
                  'title', 'meta', '[document]']


def _tag_visible(element):
    """Is this BeautifulSoup element visible on the web page?"""
    if element.parent.name in INVISIBLE_TAGS:
        return False
    if isinstance(element, Comment):
        return False
    return True


def text_from_html(html):
    """Extract visible text from HTML, as a list of strings"""
    soup = BeautifulSoup(html, config['parser'])
    texts = soup.findAll(text=True)
    visible_texts = filter(_tag_visible, texts)
    stripped_text = map(lambda x: x.extract().strip(), visible_texts)
    # Ignore single chars as well
    non_empty_text = filter(lambda x: len(x) > 1, stripped_text)
    return list(non_empty_text)


def urls_from_html(html):
    """Yield URLs in this HTML"""
    soup = BeautifulSoup(html, config['parser'])
    for link in soup.find_all('a', href=True):
        yield link['href']


async def already_done_url(url):
    """Is this URL either being processed, or has been processed?
    ------ DUMMY FUNCTION! -----
    I need to work out how to check if a
    RenderedPage already exists for this URL.

    Currently it only checks whether the page has been processed on S3,
    and not whether it is currently in queue.
    ----------------------------
    """
    bucket = config['bucket']
    async with aioboto3.resource("s3") as s3:
        bucket = await s3.Bucket(bucket)
        objs = bucket.objects.filter(Prefix=make_key(url))
        async for _ in objs:
            return True
    return False


async def put_to_s3(url, text):
    """Write the text to S3, with the file key generated
    from the URL"""
    bucket = config['bucket']
    async with aioboto3.resource("s3") as s3:
        obj = await s3.Object(bucket, make_key(url))
        await obj.put(Body=json.dumps(text))


def _make_key(url):
    """Generate a file key, by replacing forward slashes 
    with vertical bars (otherwise S3 interprets the key as a path)"""
    subfolder = config['user']
    return f'{subfolder}/{url.replace("/","|")}.json'


def skip_this_url(url):
    """Apply "quality" criteria to the URL, to ensure that it's worth
    processing. Returns False if the URL should be skipped."""
    is_email_address = ("@" in url) or ('mailto' in url)
    is_not_http = (not (url.startswith('http://')
                        or url.startswith('https://')))
    is_file = (any(url.endswith(f'.{sfx}')
                   for sfx in config['file_suffixes'])
               or any(url.endswith(f'.{sfx.upper()}')
                      for sfx in config['file_suffixes']))
    return any((is_email_address, is_not_http, is_file))

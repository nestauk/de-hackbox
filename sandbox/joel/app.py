# TODO: Faust app which is seeded by a given name:url (e.g. University of Manchester: manchester.ac.uk)
# TODO: Topics: UrlProcessor, RenderedText, StatusCode
# TODO: Table: StatusCodeTable
# TODO: An agent on RenderedText
# TODO: An agent on UrlProcessor

import requests
from bs4 import BeautifulSoup
from bs4.element import Comment

MAX_DEPTH=2

def _tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def _text_from_html(body):
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(_tag_visible, texts)
    stripped_text = map(lambda x: x.extract().strip(), visible_texts)
    non_empty_text = filter(lambda x: len(x) > 1, stripped_text)
    return list(non_empty_text)


def process_url(url, domain):
    """Map URL to {status code of requests} and {rendered text}"""
    r = requests.get(url)
    if r.status_code == 200:
        RenderedText.send(url, domain=domain, text=r.content.decode('utf-8'), depth=depth)
    StatusCodes.send(url, domain=domain, status_code=r.status_code)
    StatusCodeTable[r.status_code] += 1


def process_text(url, domain, text, depth):
    """"""
    text = _text_from_html(text)
    # TODO: boto3.dump(text)
    if depth == MAX_DEPTH:
        return
    async for next_url in _urls_from_html(text)
        # TODO: IF URL NOT IN QUEUE ALREADY
        UrlProcessor.send(next_url, domain=domain, depth=depth+1, parent=url)


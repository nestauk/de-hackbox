import requests
import lxml
from bs4 import BeautifulSoup
from bs4.element import Comment

INVISIBLE_NAMES = ['style', 'script', 'head', 'title', 'meta', '[document]']


def _tag_visible(element):
    """ Checks if element is visible
    """
    if element.parent.name in INVISIBLE_NAMES:
        return False
    if isinstance(element, Comment):
        return False
    return True


def _text_from_html(body):
    """ Gets text from body of html
    """
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(_tag_visible, texts)
    stripped_text = map(lambda x: x.extract().strip(), visible_texts)
    non_empty_text = filter(lambda x: len(x) > 1, stripped_text)
    return list(non_empty_text)


def _urls_from_html(html):
    """ Retrieves URLs from HTML
    """
    soup = BeautifulSoup(html, 'lxml')
    for link in soup.find_all('a', href=True):
        yield link['href']
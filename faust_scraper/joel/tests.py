from unittest import mock

from utils import (INVISIBLE_TAGS, _make_key, _tag_visible, skip_this_url,
                   text_from_html, urls_from_html)


def _make_element(name):
    element = mock.Mock()
    element.parent.name = name
    return element


def test__tag_visible():
    assert not any(_tag_visible(_make_element(name))
                   for name in INVISIBLE_TAGS)
    assert all(_tag_visible(_make_element(name))
               for name in ['foo', 'bar'])


def test_text_from_html():
    with open("test.html") as f:
        body = f.read()
    text = text_from_html(body)
    assert '' not in text
    assert len(text) == 332
    assert text[1] == 'Skip to main content'
    assert text[12] == 'Undergraduate'
    assert text[123] == '– research beacon'
    assert text[175] == ('The safety and wellbeing of our'
                         ' students, staff and visitors are'
                         ' our highest priority. For the'
                         ' latest guidance and updates,'
                         ' including information on testing'
                         ' for students and travel home,'
                         ' visit our')


def test_urls_from_html():
    with open("test.html") as f:
        body = f.read()
    urls = list(urls_from_html(body))
    assert len(urls) == 217
    assert urls[1] == "#content"
    assert urls[12] == "/study/undergraduate/prospectus-2021/"
    assert urls[123] == "/connect/alumni/"


def test__make_key():
    url = 'https://something.com/example'
    key = _make_key(url)
    assert url not in key
    assert key.endswith('https:||something.com|example.json')


def test_skip_this_url():
    assert skip_this_url('me@example.com')
    assert skip_this_url('mailto:me')
    assert skip_this_url('htt://example.com')
    assert skip_this_url('ftp://example.com')
    assert skip_this_url('example.com')
    assert skip_this_url('https://example.com/something.jpg')
    assert skip_this_url('https://example.com/something.DOC')

    assert not skip_this_url('https://example.com')
    assert not skip_this_url('https://www.example.com')
    assert not skip_this_url('https://example.com/something')
    assert not skip_this_url('http://example.com/something')

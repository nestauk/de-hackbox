from unittest import mock
from app import INVISIBLE_NAMES
from app import _tag_visible
from app import _text_from_html
from app import process_url
from app import process_text


def _make_element(name):
    element = mock.Mock()
    element.parent.name = name
    return element


def test__tag_visible():
    assert not any(_tag_visible(_make_element(name))
                   for name in INVISIBLE_NAMES)
    assert all(_tag_visible(_make_element(name))
               for name in ['foo', 'bar'])


def test__text_from_html():
    with open("test.html") as f:
        body = f.read()
    text = _text_from_html(body)
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
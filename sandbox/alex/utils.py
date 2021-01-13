from typing import Optional, List
from urllib3.util.url import parse_url, Url

import aioboto3
import toolz.curried as t
from botocore.client import ClientError
from bs4 import BeautifulSoup, element


async def in_s3(bucket, key):
    """Check whether `s3://<bucket>/<key>` exists."""
    async with aioboto3.resource("s3") as s3:
        bucket = await s3.Object(bucket, key)

        try:
            await bucket.metadata
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                return False
            raise exc


def extract_internal_links(soup: BeautifulSoup, host: Url) -> List[Url]:
    """Extract URL's internal (based on `host`)."""
    is_internal_link_ = is_internal_link(host)
    form_full_url_ = form_full_url(host)
    return t.pipe(
        soup.find_all("a"),
        t.map(
            lambda a: parse_url(get_href(a))
        ),  # TODO: what if this fails to parse (log and return None?)
        t.filter(None),
        t.filter(is_internal_link_),
        t.map(form_full_url_),
        set,
        list,
    )


def get_href(a: element.Tag) -> Optional[str]:
    """Get "href" attribute from anchor tag (if exists)."""
    try:
        return a.attrs["href"]
    except KeyError:
        return None


@t.curry
def is_internal_link(host_url: Url, url: Url) -> bool:
    """Determine whether `url` is an internal link to `host_url`."""
    if url.scheme in ["mailto", "tel"]:
        return False
    elif (not url.netloc and url.path) or (host_url.netloc == url.netloc):
        # Share same netloc/host OR url has not netloc but has relative path
        # TODO: test url.path begins with '/' ?
        return True
    else:
        return False


@t.curry
def form_full_url(host: Url, link: Url) -> Url:
    """Merge properties of `host` and `link` to form `link` into complete url."""
    return Url(
        **t.merge(
            host._asdict(), t.valfilter(lambda val: val is not None, link._asdict())
        )
    )

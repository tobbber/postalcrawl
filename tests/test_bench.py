import time

from parsel import Selector

from tests.conftest import read_file


def parse_then_filter(html: str):
    s = Selector(text=html)
    ldjsons = s.xpath('//script[@type="application/ld+json"]/text()').getall()
    return any("postaladdress" in s.lower() for s in ldjsons)


def test_bench2(resources_dir):
    html = read_file(resources_dir / "response.1.html")
    gen = (html for _ in range(30_000))

    start = time.perf_counter()
    sum(("postaladdress" in s.lower()) for s in gen)
    elapsed = time.perf_counter() - start
    assert elapsed == 0


def test_bench(resources_dir):
    html = read_file(resources_dir / "response.1.html")
    gen = (html for _ in range(30_000))

    start = time.perf_counter()
    sum(parse_then_filter(s) for s in gen)
    elapsed = time.perf_counter() - start
    assert elapsed == 0

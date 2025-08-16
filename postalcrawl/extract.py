from typing import Iterator
from parsel import Selector
import json

def extract_ld_json(content: str) -> Iterator[str]:
    selector = Selector(text=content)
    if selector.type not in ["html", "xml", 'text']:
        print(selector.type)
    linked_jsons= selector.xpath("//script[@type='application/ld+json']/text()").getall()

    for item in linked_jsons:
        yield item.strip()
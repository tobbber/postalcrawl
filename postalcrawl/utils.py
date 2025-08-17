import re
from pathlib import Path

import requests
from tqdm import tqdm


def download_file_id(file_id: str, dest_path: Path):
    url = "https://data.commoncrawl.org/" + file_id
    resp = requests.get(url, stream=True)
    with open(dest_path, "wb") as f:
        for data in tqdm(resp.iter_content(chunk_size=1024), unit="mB"):
            f.write(data)
    return dest_path


def file_segment_info(file_id: str) -> tuple[str, str]:
    splits = file_id.split("/")
    segment = splits[3]
    sub_id = splits[-1]
    segment_number = re.search(r"-(\d{5})\.warc\.gz$", sub_id).group(1)  # pyright: ignore [reportOptionalMemberAccess]
    return segment, segment_number

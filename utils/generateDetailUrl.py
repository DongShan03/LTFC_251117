import hashlib
import requests
import math
import re, os
import time
import urllib.parse
from pathlib import Path

_CAG_HOST = "b49b4d8a45b8f098ba881d98abbb5c892f8b5c98"
_RT_PATTERN = re.compile(r"^(http.*//[^/]*)(/.*\.(jpg|jpeg))\?*(.*)$", re.IGNORECASE)
BASE_TILE_URL = "https://cag.ltfc.net/cagstore/{id}/17/{x}_{y}.jpg"
OUTPUT_DIR = Path(__file__).resolve().parent / "tiles"

_BUCKET_MS = 31_536_000_000      # 对应 31536e6
_MULTIPLIER = 31_536_000        # 对应 31536e3


def _current_bucket_hex() -> str:
    now_ms = int(time.time() * 1000)
    value = math.ceil(now_ms / _BUCKET_MS) * _MULTIPLIER
    return format(value, "x")

def get_detail_url(url: str) -> str:
    match = _RT_PATTERN.match(url)
    if not match:
        return url

    base = match.group(1)
    path = match.group(2)
    query = match.group(4) or ""

    timestamp_hex = _current_bucket_hex()
    payload = _CAG_HOST + urllib.parse.quote(path, safe="/:@&=+$,-_.!~*'()#") + timestamp_hex
    sign = hashlib.md5(payload.encode("utf-8")).hexdigest()

    return f"{base}{path}?{query}&sign={sign}&t={timestamp_hex}"

def fetch_tile(id: str, x: int, y: int):
    base = BASE_TILE_URL.format(id=id, x=x, y=y)
    url = get_detail_url(base)
    print(url)
    response = requests.get(url, timeout=20)

    if response.status_code == 200 and response.headers.get("Content-Type", "").startswith("image"):
        tile_path = OUTPUT_DIR / id / f"{x}_{y}.jpg"
        os.makedirs(tile_path.parent, exist_ok=True)
        tile_path.write_bytes(response.content)
        print(f"saved tile {tile_path}")
        return tile_path

    try:
        data = response.json()
    except ValueError:
        data = None

    message = data.get("error") if isinstance(data, dict) else response.text
    print(f"stop at id={id}, x={x}, y={y}: status={response.status_code}, message={message}")
    return None

def fetch_all_tile(id: str):
    x = 0
    max_y_limit = None
    while True:
        y = 0
        any_success = False
        while True:
            if max_y_limit is not None and y >= max_y_limit:
                break
            result = fetch_tile(id, x, y)
            if result is None:
                if max_y_limit is None:
                    max_y_limit = y
                if not any_success:
                    print("当前 x 无有效切片，结束。")
                    return
                break
            any_success = True
            y += 1
        x += 1

if __name__ == "__main__":
    fetch_tile("5d8661933b6f2b1026b00167", 19, 0)

from pathlib import Path
from typing import Optional

import requests

from generateDetailUrl import get_detail_url

BASE_TILE_URL = "https://cag.ltfc.net/cagstore/5be3970c8ed7f411e26a5647/17/{x}_{y}.jpg"
OUTPUT_DIR = Path("tiles")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def fetch_tile(x: int, y: int) -> Optional[Path]:
    base_url = BASE_TILE_URL.format(x=x, y=y)
    signed_url = get_detail_url(base_url)
    response = requests.get(signed_url, timeout=20)

    if response.status_code == 200 and response.headers.get("Content-Type", "").startswith("image"):
        tile_path = OUTPUT_DIR / f"{x}_{y}.jpg"
        tile_path.write_bytes(response.content)
        print(f"保存切片 {tile_path}")
        return tile_path

    try:
        data = response.json()
        message = data.get("error")
    except ValueError:
        message = response.text

    print(f"x={x}, y={y} 停止：status={response.status_code}, message={message}")
    return None


def main() -> None:
    x = 0
    while True:
        y = 0
        any_success = False
        while True:
            result = fetch_tile(x, y)
            if result is None:
                if not any_success:
                    print("当前 x 无有效切片，结束。")
                    return
                break
            any_success = True
            y += 1
        x += 1


if __name__ == "__main__":
    main()

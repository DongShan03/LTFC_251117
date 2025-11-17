from pathlib import Path
from typing import Optional

import requests

from generateDetailUrl import get_detail_url

BASE_TILE_URL = "https://cag.ltfc.net/cagstore/{resource_id}/17/{x}_{y}.jpg"

def fetch_tile(output_path, resource_id: str, x: int, y: int) -> Optional[Path]:
    base_url = BASE_TILE_URL.format(resource_id=resource_id, x=x, y=y)
    signed_url = get_detail_url(base_url)
    response = requests.get(signed_url, timeout=20)

    if response.status_code == 200 and response.headers.get("Content-Type", "").startswith("image"):
        tile_path = output_path / f"{x}_{y}.jpg"
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

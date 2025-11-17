import csv
import json
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple

import requests
from faker import Faker

ua = Faker()

HEADERS = {
    'accept': 'application/json',
    'accept-language': 'zh-CN,zh;q=0.9',
    'content-type': 'application/json;charset=UTF-8',
    'origin': 'https://g2.ltfc.net',
    'referer': 'https://g2.ltfc.net/',
    'user-agent': ua.user_agent()
}

Record = Dict[str, Any]

API_URL = "https://api.quanku.art/cag2.ArtistService/list"
REQUEST_TIMEOUT = 30
PAGE_SIZE = 3358
TOTAL_RECORDS = 3358
THROTTLE_SECONDS = 3

PAYLOAD_TEMPLATE = {
    "page": {
        "skip": 0,
        "limit": PAGE_SIZE,
    },
    "context": {
        "tourToken": "",
    },
}

PREFERRED_FIELD_ORDER = [
    "Id",
    "name",
    "py",
    "age",
    "alias",
    "lifeTime",
    "startAge",
    "endAge",
    "homeTown",
    "houseName",
    "country",
    "worksCount",
    "desc",
    "content",
    "tags",
    "category",
    "searchAlias",
    "modelOrigin",
    "avatar",
    "representSnapUrl",
    "representThumbTileUrl",
    "representRes",
    "otherAvatars",
    "spelling",
    "auditState",
    "auditMessage",
    "deleted",
    "ctime",
    "utime",
]

OUTPUT_DIR = Path(__file__).resolve().parent
OUTPUT_JSON = OUTPUT_DIR / "artists.json"
OUTPUT_CSV = OUTPUT_DIR / "artists.csv"

def init():
    response = requests.post('https://api.quanku.art/cag2.TouristService/getAccessToken', headers=HEADERS)
    PAYLOAD_TEMPLATE["context"]["tourToken"] = response.json()['token']

def build_payload(skip: int) -> Dict[str, Any]:
    """基于模板构造分页请求体。"""
    payload = deepcopy(PAYLOAD_TEMPLATE)
    payload["page"]["skip"] = skip
    return payload


def load_existing_data() -> Tuple[List[Record], int]:
    """读取本地缓存的抓取结果。"""
    if not OUTPUT_JSON.exists():
        return [], TOTAL_RECORDS

    try:
        payload = json.loads(OUTPUT_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"读取已有数据失败（{exc}），将重新抓取。")
        return [], TOTAL_RECORDS

    data = payload.get("data", [])
    total = payload.get("total", TOTAL_RECORDS)

    if not isinstance(data, list):
        print("已有 response.json 数据格式异常，忽略并重新抓取。")
        return [], TOTAL_RECORDS

    sanitized_data = [item for item in data if isinstance(item, dict)]
    total_value = total if isinstance(total, int) else TOTAL_RECORDS
    return sanitized_data, total_value


def fetch_page(skip: int) -> Tuple[List[Record], int]:
    """抓取单页数据，返回清洗后的记录以及服务端宣称的总数。"""
    payload = build_payload(skip)

    response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    body = response.json()

    page_records = body.get("data", [])
    if not isinstance(page_records, list):
        raise ValueError("返回结构异常：`data` 字段不是列表")

    sanitized_records = [item for item in page_records if isinstance(item, dict)]
    total = body.get("total", TOTAL_RECORDS)
    return sanitized_records, total if isinstance(total, int) else TOTAL_RECORDS


def flatten_value(value: Any) -> str:
    """将任意对象转为 CSV 友好的字符串。"""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "|".join(flatten_value(item) for item in value)
    return json.dumps(value, ensure_ascii=False)


def collect_fieldnames(records: Iterable[Record]) -> List[str]:
    """根据优先顺序生成 CSV 列名列表。"""
    key_set = {key for record in records for key in record.keys()}
    fieldnames: List[str] = []

    for field in PREFERRED_FIELD_ORDER:
        if field in key_set:
            fieldnames.append(field)
            key_set.remove(field)

    fieldnames.extend(sorted(key_set))
    return fieldnames


def write_json(records: List[Record], total: int) -> None:
    """以 JSON 形式写入抓取结果。"""
    OUTPUT_JSON.write_text(
        json.dumps({"data": records, "total": total}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_csv(records: List[Record]) -> None:
    """以 CSV 形式写入抓取结果。"""
    if not records:
        OUTPUT_CSV.write_text("", encoding="utf-8")
        return

    fieldnames = collect_fieldnames(records)

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            flattened = {field: flatten_value(record.get(field)) for field in fieldnames}
            writer.writerow(flattened)


def persist_progress(records: List[Record], total: int) -> None:
    """将当前抓取进度同步到磁盘。"""
    normalized_total = total if isinstance(total, int) and total > 0 else len(records)
    write_json(records, normalized_total)
    write_csv(records)


def fetch_all(initial_records: List[Record], initial_total: int) -> Tuple[List[Record], int]:
    """在断点续传基础上持续抓取，直到达到服务端声明的总数。"""
    records: List[Record] = [item for item in initial_records if isinstance(item, dict)]
    seen_ids: Set[Any] = {record.get("Id") for record in records if "Id" in record}

    expected_total = initial_total or TOTAL_RECORDS
    skip = len(records)

    while skip < expected_total:
        page_records, page_total = fetch_page(skip)
        if not page_records:
            break

        for item in page_records:
            item_id = item.get("Id") if isinstance(item, dict) else None
            if item_id in seen_ids:
                continue
            records.append(item)
            if item_id is not None:
                seen_ids.add(item_id)

        expected_total = page_total if isinstance(page_total, int) else expected_total
        skip += len(page_records)

        persist_progress(records, expected_total)
        time.sleep(THROTTLE_SECONDS)

    return records, expected_total


def main() -> None:
    init()
    """脚本入口：加载缓存，抓取数据并写出结果。"""
    existing_records, existing_total = load_existing_data()
    if existing_records:
        print(f"检测到已完成 {len(existing_records)} 条数据，继续抓取剩余部分。")

    records, total = fetch_all(existing_records, existing_total)
    persist_progress(records, total)
    print(f"共获取 {len(records)} 条数据，目标总数 {total}，已写入 {OUTPUT_CSV.name}")


if __name__ == "__main__":
    main()

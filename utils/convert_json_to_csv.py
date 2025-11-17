import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

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

ROOT_DIR = Path(__file__).resolve().parent
INPUT_JSON = ROOT_DIR / "artists.json"
OUTPUT_CSV = ROOT_DIR / "artists.csv"


def flatten_value(value: Any) -> str:
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


def collect_fieldnames(records: Iterable[Dict[str, Any]]) -> List[str]:
    key_set = {key for record in records for key in record.keys()}
    fieldnames: List[str] = []

    for field in PREFERRED_FIELD_ORDER:
        if field in key_set:
            fieldnames.append(field)
            key_set.remove(field)

    fieldnames.extend(sorted(key_set))
    return fieldnames


def main() -> None:
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"未找到 {INPUT_JSON}")

    payload = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    raw_records = payload.get("data", [])
    records = [item for item in raw_records if isinstance(item, dict)]

    if not records:
        OUTPUT_CSV.write_text("", encoding="utf-8")
        print("response.json 中未找到有效数据，已创建空的 artists.csv")
        return

    fieldnames = collect_fieldnames(records)

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            flattened = {field: flatten_value(record.get(field)) for field in fieldnames}
            writer.writerow(flattened)

    print(f"共写入 {len(records)} 条记录到 {OUTPUT_CSV.name}")


if __name__ == "__main__":
    main()

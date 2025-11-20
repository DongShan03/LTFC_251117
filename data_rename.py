import csv
import json
import logging
import re
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from PIL import Image
    Image.MAX_IMAGE_PIXELS = None  # 允许处理大图
except ImportError as exc:  # pragma: no cover - 运行前检查依赖
    raise SystemExit("需要安装 Pillow 库 (pip install Pillow)") from exc


RAW_DATA_DIR = Path("data/rawdata")
CLEANED_DATA_DIR = Path("data/cleanedData")
ARTIST_CSV = Path("data/artists.csv")
MERGED_TILE_NAME = "merged.jpg"
TILE_PATTERN = re.compile(r"^(?P<x>\d+)_(?P<y>\d+)\.(?P<ext>jpg|jpeg|png)$", re.IGNORECASE)
INVALID_FS_CHARS = re.compile(r'[\\/:*?"<>|]')
MAX_PIXELS_WARNING = 300_000_000  # 超过该像素数提示可能内存不足

logger = logging.getLogger("data_rename")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def sanitize_name(name: str, fallback: str) -> str:
    candidate = INVALID_FS_CHARS.sub("_", name).strip()
    candidate = candidate.replace("\u3000", " ").strip()
    return candidate or fallback


def ensure_unique(name: str, used: Dict[str, int]) -> str:
    counter = used.get(name, 0)
    if counter == 0:
        used[name] = 1
        return name
    new_name = f"{name}_{counter}"
    while new_name in used:
        counter += 1
        new_name = f"{name}_{counter}"
    used[name] = counter + 1
    used[new_name] = 1
    return new_name


def load_artist_names(csv_path: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            artist_id = (row.get("Id") or "").strip()
            name = (row.get("name") or "").strip()
            if artist_id:
                mapping[artist_id] = name or artist_id
    return mapping


def load_work_name_map(artist_dir: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for file_name in ("all_huia_of_artist.json", "all_sufa_of_artist.json"):
        json_path = artist_dir / file_name
        if not json_path.exists():
            continue
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            logger.warning("解析 %s 失败: %s", json_path, exc)
            continue
        for item in payload.get("data", []):
            if not isinstance(item, dict):
                continue
            work_id = item.get("Id")
            work_name = item.get("name") or item.get("title") or work_id
            if work_id:
                mapping[work_id] = work_name or work_id
    return mapping


def _collect_named_entries(container: Iterable) -> List[Tuple[str, str]]:
    results: List[Tuple[str, str]] = []
    for entry in container:
        if isinstance(entry, dict):
            candidate_id = entry.get("Id") or entry.get("id") or entry.get("resourceId")
            candidate_name = entry.get("name") or entry.get("title") or entry.get("resourceName")
            if candidate_id:
                results.append((candidate_id, candidate_name or candidate_id))
            for key in ("suha", "sufa", "hdp", "hdpic", "resource", "pic"):
                nested = entry.get(key)
                if isinstance(nested, dict):
                    nested_id = nested.get("Id") or nested.get("id") or nested.get("resourceId")
                    nested_name = nested.get("name") or nested.get("title") or nested.get("resourceName")
                    if nested_id:
                        results.append((nested_id, nested_name or nested_id))
                elif isinstance(nested, list):
                    results.extend(_collect_named_entries(nested))
        elif isinstance(entry, list):
            results.extend(_collect_named_entries(entry))
    return results


def load_resource_name_map(sub_list_path: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not sub_list_path.exists():
        return mapping
    try:
        payload = json.loads(sub_list_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.warning("解析 %s 失败: %s", sub_list_path, exc)
        return mapping

    data_section = payload.get("data", [])
    collected = _collect_named_entries(data_section if isinstance(data_section, list) else [])

    parent_data = payload.get("parentData")
    if isinstance(parent_data, dict):
        collected.extend(_collect_named_entries([parent_data]))

    for resource_id, resource_name in collected:
        mapping.setdefault(resource_id, resource_name)
    return mapping


def extract_variant_name_map(resource_json_path: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not resource_json_path.exists():
        return mapping
    try:
        payload = json.loads(resource_json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.warning("解析 %s 失败: %s", resource_json_path, exc)
        return mapping

    data = payload.get("data")
    if not isinstance(data, dict):
        return mapping

    for key in ("suha", "sufa"):
        info = data.get(key)
        if not isinstance(info, dict):
            continue
        hdp_info = info.get("hdp")
        other_hdps = info.get("otherHdps") or []
        mapping.update(_extract_from_hdp(hdp_info))
        mapping.update(_extract_from_hdp_collection(other_hdps))
    return mapping


def _extract_from_hdp(hdp_info: Optional[dict]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not isinstance(hdp_info, dict):
        return mapping
    hdpic = hdp_info.get("hdpic")
    if isinstance(hdpic, dict):
        rid = hdpic.get("resourceId")
        name = hdpic.get("name") or hdpic.get("title")
        if rid:
            mapping[rid] = name or rid
    hdpcoll = hdp_info.get("hdpcoll")
    if isinstance(hdpcoll, dict):
        for item in hdpcoll.get("hdps", []):
            if not isinstance(item, dict):
                continue
            rid = item.get("resourceId")
            name = item.get("name") or item.get("title")
            if rid:
                mapping[rid] = name or rid
    return mapping


def _extract_from_hdp_collection(items: Iterable) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for item in items or []:
        if not isinstance(item, dict):
            continue
        rid = item.get("resourceId")
        name = item.get("name") or item.get("title")
        if rid:
            mapping[rid] = name or rid
    return mapping


def copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def merge_tiles(tile_dir: Path, output_path: Path) -> None:
    tile_files = [p for p in tile_dir.iterdir() if p.is_file() and TILE_PATTERN.match(p.name)]
    if not tile_files:
        logger.info("目录 %s 中没有可合并的瓦片", tile_dir)
        return

    coords = []
    for tile in tile_files:
        match = TILE_PATTERN.match(tile.name)
        if not match:
            continue
        x = int(match.group("x"))
        y = int(match.group("y"))
        coords.append((x, y, tile))

    if not coords:
        logger.info("目录 %s 中未匹配到有效瓦片命名", tile_dir)
        return

    coords.sort()
    with Image.open(coords[0][2]) as first_tile:
        tile_w, tile_h = first_tile.size
    max_x = max(x for x, _, _ in coords)
    max_y = max(y for _, y, _ in coords)
    final_width = (max_x + 1) * tile_w
    final_height = (max_y + 1) * tile_h
    total_pixels = final_width * final_height

    if total_pixels > MAX_PIXELS_WARNING:
        logger.warning(
            "目标图像尺寸 %s x %s (%.2f MP) 很大，可能需要大量内存",
            final_width,
            final_height,
            total_pixels / 1_000_000,
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    canvas = Image.new("RGB", (final_width, final_height), color=(255, 255, 255))
    for x, y, tile_file in coords:
        try:
            with Image.open(tile_file) as img:
                canvas.paste(img.convert("RGB"), (x * tile_w, y * tile_h))
        except OSError as exc:
            logger.warning("读取瓦片 %s 失败: %s", tile_file, exc)
    canvas.save(output_path, quality=95)
    logger.info("已生成合并图像: %s", output_path)


def process_artist(artist_dir: Path, artist_name_map: Dict[str, str], used_artist_names: Dict[str, int]) -> None:
    artist_id = artist_dir.name
    artist_display = artist_name_map.get(artist_id, artist_id)
    sanitized_artist = sanitize_name(artist_display, artist_id)
    artist_folder_name = ensure_unique(sanitized_artist, used_artist_names)
    target_artist_dir = CLEANED_DATA_DIR / artist_folder_name
    target_artist_dir.mkdir(parents=True, exist_ok=True)

    for meta_name in ("all_huia_of_artist.json", "all_sufa_of_artist.json"):
        src_meta = artist_dir / meta_name
        if src_meta.exists():
            copy_file(src_meta, target_artist_dir / meta_name)

    work_name_map = load_work_name_map(artist_dir)
    used_work_names: Dict[str, int] = defaultdict(int)

    for work_dir in sorted(p for p in artist_dir.iterdir() if p.is_dir()):
        work_id = work_dir.name
        work_display = work_name_map.get(work_id, work_id)
        work_name = ensure_unique(sanitize_name(work_display, work_id), used_work_names)
        target_work_dir = target_artist_dir / work_name
        target_work_dir.mkdir(parents=True, exist_ok=True)

        sub_list_path = work_dir / "sub_list.json"
        if sub_list_path.exists():
            copy_file(sub_list_path, target_work_dir / "sub_list.json")
        resource_name_map = load_resource_name_map(sub_list_path)
        used_resource_names: Dict[str, int] = defaultdict(int)

        for resource_dir in sorted(p for p in work_dir.iterdir() if p.is_dir()):
            resource_id = resource_dir.name
            resource_display = resource_name_map.get(resource_id, resource_id)
            resource_name = ensure_unique(sanitize_name(resource_display, resource_id), used_resource_names)
            target_resource_dir = target_work_dir / resource_name
            target_resource_dir.mkdir(parents=True, exist_ok=True)

            resource_json_path = resource_dir / "resource.json"
            if resource_json_path.exists():
                copy_file(resource_json_path, target_resource_dir / "resource.json")
            variant_name_map = extract_variant_name_map(resource_json_path)
            used_variant_names: Dict[str, int] = defaultdict(int)

            for child_dir in sorted(p for p in resource_dir.iterdir() if p.is_dir()):
                variant_id = child_dir.name
                variant_display = variant_name_map.get(variant_id, variant_id)
                variant_name = ensure_unique(sanitize_name(variant_display, variant_id), used_variant_names)
                target_variant_dir = target_resource_dir / variant_name
                target_variant_dir.mkdir(parents=True, exist_ok=True)

                tile_dir = child_dir / "tile"
                if tile_dir.is_dir():
                    try:
                        merge_tiles(tile_dir, target_variant_dir / MERGED_TILE_NAME)
                    except Exception as exc:  # pragma: no cover - 捕获合并运行异常
                        logger.warning("合并 %s 瓦片失败: %s", tile_dir, exc)
                else:
                    logger.info("目录 %s 缺少 tile 子目录，跳过合并", child_dir)


def main() -> None:
    if not RAW_DATA_DIR.exists():
        raise SystemExit(f"未找到原始数据目录: {RAW_DATA_DIR}")
    if not ARTIST_CSV.exists():
        raise SystemExit(f"未找到艺术家 CSV: {ARTIST_CSV}")

    artist_name_map = load_artist_names(ARTIST_CSV)
    CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    used_artist_names: Dict[str, int] = defaultdict(int)
    for artist_dir in sorted(p for p in RAW_DATA_DIR.iterdir() if p.is_dir()):
        logger.info("处理艺术家: %s", artist_dir.name)
        process_artist(artist_dir, artist_name_map, used_artist_names)

    logger.info("处理完成，输出目录: %s", CLEANED_DATA_DIR.resolve())


if __name__ == "__main__":
    main()

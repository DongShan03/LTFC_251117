import hashlib
import json
import logging
import math
import subprocess
import os
import re
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from pathlib import Path
from typing import Dict, List, Optional

import coloredlogs
import pandas as pd
import requests
from faker import Faker
from tqdm import tqdm

USE_PROXY = False
ONE_IMAGE_PER_WORK = False

ua = Faker()

ACCESS_TOKEN_URL = "https://api.quanku.art/cag2.TouristService/getAccessToken"
ALL_HUIA_OF_ARTIST_URL = "https://api.quanku.art/cag2.ArtistService/listHuiaOfArtist"
ALL_SUFA_OF_ARTIST_URL = "https://api.quanku.art/cag2.ArtistService/listSufaOfArtist"
SUB_LIST_URL = "https://api.quanku.art/cag2.ResourceService/getSubList"
RESOURCE_ID_URL = "https://api.quanku.art/cag2.ResourceService/getResource"
BASE_TILE_URL = "https://cag.ltfc.net/cagstore/{resource_id}/17/{x}_{y}.jpg"

OUTPUT_DIR = Path(__file__).resolve().parent / "data"
RAWDATA_DIR = OUTPUT_DIR / "rawdata"

DEFAULT_TIMEOUT = 20

_CAG_HOST = "b49b4d8a45b8f098ba881d98abbb5c892f8b5c98"
_RT_PATTERN = re.compile(r"^(http.*//[^/]*)(/.*\.(jpg|jpeg))\?*(.*)$", re.IGNORECASE)


_BUCKET_MS = 31_536_000_000  # 对应 31536e6
_MULTIPLIER = 31_536_000  # 对应 31536e3

logger = logging.getLogger(__name__)
coloredlogs.install(level="INFO", logger=logger)


def _normalize_proxy(proxy: Dict[str, str] | str) -> Dict[str, str]:
    if isinstance(proxy, str):
        proxy = {"http": proxy, "https": proxy}
    if not isinstance(proxy, dict):
        raise ValueError(f"非法代理配置: {proxy}")
    cleaned = {k: v for k, v in proxy.items() if v}
    if not cleaned:
        raise ValueError("代理配置为空")
    return cleaned


def _request_json(method: str, url: str, *, timeout: int = DEFAULT_TIMEOUT, **kwargs) -> Dict:
    try:
        response = requests.request(method, url, timeout=timeout, **kwargs)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"{method.upper()} {url} 请求异常: {exc}") from exc
    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError(f"{method.upper()} {url} 响应非 JSON: {exc}") from exc


def _safe_write_json(path: Path, payload: Dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)
    except OSError as exc:
        logger.error("写入文件失败 %s: %s", path, exc)


class ProxyAgent:
    def __init__(self, proxy: Dict[str, str] | str | None = None):
        if USE_PROXY:
            if proxy is None:
                raise ValueError("未提供代理配置")
            self.proxy = _normalize_proxy(proxy)
        else:
            self.proxy = {}
        self.headers = {
            "accept": "application/json",
            "accept-language": "zh-CN,zh;q=0.9",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://g2.ltfc.net",
            "referer": "https://g2.ltfc.net/",
            "user-agent": ua.user_agent(),
        }
        self.tour_token = self._get_token()

    def _get_token(self) -> str:
        payload = _request_json(
            "post",
            ACCESS_TOKEN_URL,
            headers=self.headers,
            proxies=self.proxy or None,
            timeout=DEFAULT_TIMEOUT,
        )
        token = payload.get("token") if isinstance(payload, dict) else None
        if not token:
            raise RuntimeError(f"响应中缺少 token 字段: {payload}")
        return token


class LTFCDownload:
    def __init__(self, artist_csv: str, num: int = 75):
        self.artist_csv = artist_csv
        self.artists_info = pd.read_csv(self.artist_csv)
        self.artists_id = self.artists_info["Id"].tolist()
        self.num = num
        self.second_agent_usage = 0

        if USE_PROXY:
            self.key = os.getenv("QINGGOU_KEY", "YOUR_TOKEN_HERE")
            if not self.key:
                raise ValueError("Proxy key 未配置，请设置环境变量 QINGGOU_KEY")
            self.agent_list = self._get_agent_list(self.key, self.num)
            self.second_agent_list = self._get_agent_list(self.key, max(self.num * 3, 200))
        else:
            self.key = ""
            agent = ProxyAgent(None)
            self.agent_list = [agent]
            self.second_agent_list = [agent]

    def _resource_root(self, artist_id: str, work_id: str, parent_resource_id: str, child_resource_id: Optional[str] = None) -> Path:
        base = RAWDATA_DIR / artist_id / work_id / parent_resource_id
        if child_resource_id:
            return base / child_resource_id
        return base

    def _resource_flag_path(self, artist_id: str, work_id: str, parent_resource_id: str, child_resource_id: str) -> Path:
        return self._resource_root(artist_id, work_id, parent_resource_id, child_resource_id) / ".completed"

    def _tile_dir(self, artist_id: str, work_id: str, parent_resource_id: str, child_resource_id: str) -> Path:
        return self._resource_root(artist_id, work_id, parent_resource_id, child_resource_id) / "tile"

    def _is_resource_completed(self, artist_id: str, work_id: str, parent_resource_id: str, child_resource_id: str) -> bool:
        return self._resource_flag_path(artist_id, work_id, parent_resource_id, child_resource_id).exists()

    def _mark_resource_completed(self, artist_id: str, work_id: str, parent_resource_id: str, child_resource_id: str) -> None:
        flag_path = self._resource_flag_path(artist_id, work_id, parent_resource_id, child_resource_id)
        try:
            flag_path.parent.mkdir(parents=True, exist_ok=True)
            flag_path.write_text(str(int(time.time())), encoding="utf-8")
        except OSError as exc:
            logger.warning("写入完成标记失败 %s: %s", flag_path, exc)

    def _get_agent_list(self, key: str, num: int) -> List[ProxyAgent]:
        if not USE_PROXY:
            return [ProxyAgent(None)]

        proxy_url = f"https://proxy.qg.net/allocate?Key={key}&Num={num}"
        payload = _request_json("get", proxy_url, timeout=DEFAULT_TIMEOUT)
        data = payload.get("Data") if isinstance(payload, dict) else None
        if not data:
            raise RuntimeError(f"代理服务未返回可用代理: {payload}")

        agents: List[ProxyAgent] = []
        for entry in data:
            host = entry.get("host") if isinstance(entry, dict) else None
            if isinstance(host, dict):
                proxy_cfg = host
            elif isinstance(host, str):
                proxy_cfg = {"http": host, "https": host}
            else:
                logger.warning("跳过无法识别的代理条目: %s", entry)
                continue
            try:
                agents.append(ProxyAgent(proxy_cfg))
            except Exception as exc:
                logger.warning("创建代理失败 %s: %s", proxy_cfg, exc)

        if not agents:
            raise RuntimeError("无法获取任何有效代理，请检查代理服务是否正常")
        return agents

    def _fetch_artist_resources(self, url: str, artist_id: str, agent: ProxyAgent) -> Dict:
        json_data = {
            "Id": artist_id,
            "page": {"skip": 0, "limit": 999},
            "context": {"tourToken": agent.tour_token},
        }
        write_file = RAWDATA_DIR / artist_id / ("all_huia_of_artist.json" if url == ALL_HUIA_OF_ARTIST_URL else "all_sufa_of_artist.json")
        try:
            payload = _request_json(
                "post",
                url,
                headers=agent.headers,
                json=json_data,
                proxies=agent.proxy,
                timeout=DEFAULT_TIMEOUT,
            )
        except RuntimeError as exc:
            logger.error("访问 %s 失败: %s", url, exc)
            _safe_write_json(write_file, {"error": str(exc), "request": json_data})
            return {"data": []}

        _safe_write_json(write_file, payload)
        return payload if isinstance(payload, dict) else {"data": []}

    def get_all_of_artist(self, artist_id: str, agent: ProxyAgent) -> tuple[List[Dict], List[Dict], str]:
        payload_painting = self._fetch_artist_resources(ALL_HUIA_OF_ARTIST_URL, artist_id, agent)
        payload_calligraphy = self._fetch_artist_resources(ALL_SUFA_OF_ARTIST_URL, artist_id, agent)

        paint_data = payload_painting.get("data") if isinstance(payload_painting, dict) else []
        sufa_data = payload_calligraphy.get("data") if isinstance(payload_calligraphy, dict) else []

        if not isinstance(paint_data, list):
            logger.warning("艺术家 %s 绘画列表数据异常: %s", artist_id, payload_painting)
            paint_data = []
        if not isinstance(sufa_data, list):
            logger.warning("艺术家 %s 书法列表数据异常: %s", artist_id, payload_calligraphy)
            sufa_data = []

        artist_name_series = self.artists_info[self.artists_info["Id"] == artist_id]["name"]
        artist_name = artist_name_series.values[0] if not artist_name_series.empty else artist_id
        return paint_data, sufa_data, artist_name

    def get_sub_list(self, artist_id: str, work: Dict, work_src: str, agent: ProxyAgent) -> tuple[List[Dict], Optional[Dict], str]:
        work_id = work.get("Id") or ""
        json_data = {
            "src": work_src,
            "id": work_id,
            "context": {"tourToken": agent.tour_token},
        }
        write_file = RAWDATA_DIR / artist_id / work_id / "sub_list.json"
        try:
            payload = _request_json(
                "post",
                SUB_LIST_URL,
                headers=agent.headers,
                json=json_data,
                proxies=agent.proxy,
                timeout=DEFAULT_TIMEOUT,
            )
        except RuntimeError as exc:
            logger.error("获取艺术家 %s 作品 %s 的子资源列表失败: %s", artist_id, work_id, exc)
            _safe_write_json(write_file, {"error": str(exc), "request": json_data})
            return [], None, work_src

        _safe_write_json(write_file, payload)
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            logger.warning("作品 %s 的子资源列表数据异常: %s", work_id, payload)
            data = []
        parent_suha = None
        parent_data = payload.get("parentData") if isinstance(payload, dict) else None
        if isinstance(parent_data, dict):
            key = "suha" if work_src == "SUHA" else "sufa"
            parent_suha = parent_data.get(key) if isinstance(parent_data.get(key), dict) else None
        return data, parent_suha, work_src

    def _extract_resource_variants(self, resource: Dict, work_src: str) -> List[tuple[str, str]]:
        variants: List[tuple[str, str]] = []
        if work_src == "SUHA":
            key = "suha"
        else:
            key = "sufa"
        info = resource.get(key) if isinstance(resource, dict) else {}
        hdp_info = info.get("hdp") if isinstance(info, dict) else {}

        # Direct hdpic entry
        hdpic = hdp_info.get("hdpic") if isinstance(hdp_info, dict) else None
        if isinstance(hdpic, dict):
            rid = hdpic.get("resourceId")
            if rid:
                name = hdpic.get("name") or info.get("name") or rid
                variants.append((rid, name, work_src))

        # Collection with multiple hdps
        hdpcoll = hdp_info.get("hdpcoll") if isinstance(hdp_info, dict) else None
        if isinstance(hdpcoll, dict):
            for item in hdpcoll.get("hdps", []) or []:
                if not isinstance(item, dict):
                    continue
                rid = item.get("resourceId")
                if not rid:
                    continue
                name = item.get("name") or item.get("title") or info.get("name") or rid
                variants.append((rid, name, work_src))

        # otherHdps array as fallback
        for extra in info.get("otherHdps", []) or []:
            if not isinstance(extra, dict):
                continue
            rid = extra.get("resourceId")
            if not rid:
                continue
            name = extra.get("name") or extra.get("title") or info.get("name") or rid
            variants.append((rid, name, work_src))

        # Deduplicate while preserving order
        seen: set[str] = set()
        uniq: List[tuple[str, str]] = []
        for rid, name, work_src in variants:
            if rid in seen:
                continue
            seen.add(rid)
            uniq.append((rid, name, work_src))
        return uniq

    def get_resource(self, artist_id: str, work_id: str, work_src: str, resource_id: str, resource_name: str, agent: ProxyAgent) -> tuple[Dict, List[tuple[str, str]]]:
        json_data = {
            "id": resource_id,
            "src": work_src,
            "context": {"tourToken": agent.tour_token},
        }
        parent_root = self._resource_root(artist_id, work_id, resource_id)
        try:
            payload = _request_json(
                "post",
                RESOURCE_ID_URL,
                headers=agent.headers,
                json=json_data,
                proxies=agent.proxy,
                timeout=DEFAULT_TIMEOUT,
            )
        except RuntimeError as exc:
            logger.error("获取资源 %s 详情失败: %s", resource_id, exc)
            return {}, []

        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            logger.warning("资源 %s 详情数据异常: %s", resource_id, payload)
            return {}, []
        _safe_write_json(parent_root / "resource.json", payload)
        variants = self._extract_resource_variants(data, work_src)
        if not variants:
            variants = [(resource_id, resource_name, work_src)]
        return data, variants

    def _current_bucket_hex(self) -> str:
        now_ms = int(time.time() * 1000)
        value = math.ceil(now_ms / _BUCKET_MS) * _MULTIPLIER
        return format(value, "x")

    def _build_sign(self, path: str, timestamp: int, start: int, end: int) -> str:
        seeds = "0134cdef"
        hex_chars = list(seeds)
        base = "lt_" + "".join(hex_chars) + "net"
        prefix, mid, suffix = base.split("_", 1)[0], "fc", base.split("_", 1)[1]
        ke_value = f"{prefix}{mid}{suffix}"
        sign_source = f"{path}-{timestamp}-{start}-{end}-{ke_value}"
        return hashlib.md5(sign_source.encode("utf-8")).hexdigest()

    def get_SUFA_detail_url(self, url: Optional[str]) -> Optional[str]:
        result = subprocess.run(["node", r"utils/get_USFA.js", "init", url], capture_output=True, text=True)
        return result.stdout.strip()

    def get_SUHA_detail_url(self, url: str) -> str:
        match = _RT_PATTERN.match(url)
        if not match:
            return url

        base = match.group(1)
        path = match.group(2)
        query = match.group(4) or ""

        timestamp_hex = self._current_bucket_hex()
        payload = _CAG_HOST + urllib.parse.quote(path, safe="/:@&=+$,-_.!~*'()#") + timestamp_hex
        sign = hashlib.md5(payload.encode("utf-8")).hexdigest()

        return f"{base}{path}?{query}&sign={sign}&t={timestamp_hex}"

    def fetch_tile(
        self,
        artist_id: str,
        artist_name: str,
        work_id: str,
        work_name: str,
        parent_resource_id: str,
        child_resource_id: str,
        x: int,
        y: int,
        agent: ProxyAgent,
        work_src: str,
    ) -> Optional[Path]:
        tile_dir = self._tile_dir(artist_id, work_id, parent_resource_id, child_resource_id)
        tile_dir.mkdir(parents=True, exist_ok=True)
        tile_path = tile_dir / f"{x}_{y}.jpg"
        if tile_path.exists():
            return tile_path

        base = BASE_TILE_URL.format(resource_id=child_resource_id, x=x, y=y)
        if work_src == "SUFA":
            url = self.get_SUFA_detail_url(base)
        else:
            url = self.get_SUHA_detail_url(base)
        print(url)

        attempts = 5 if USE_PROXY else 1
        attempt = 0
        while attempt < attempts:
            current_agent = agent
            if attempt > 0 and USE_PROXY:
                # rotate proxy
                if self.second_agent_usage == len(self.second_agent_list) * 10:
                    self.second_agent_list = self._get_agent_list(self.key, max(self.num * 3, 200))
                    self.second_agent_usage = 0
                current_agent = self.second_agent_list[self.second_agent_usage % len(self.second_agent_list)]
                self.second_agent_usage += 1

            try:
                response = requests.get(url, timeout=DEFAULT_TIMEOUT, proxies=current_agent.proxy or None)
                break
            except requests.RequestException as exc:
                logger.error(
                    "下载瓦片失败 artist=%s(%s) work=%s(%s) resource=%s (%s,%s) attempt=%s: %s",
                    artist_name,
                    artist_id,
                    work_name,
                    work_id,
                    child_resource_id,
                    x,
                    y,
                    attempt + 1,
                    exc,
                )
                attempt += 1
                if attempt >= attempts:
                    return None
                continue

        if response.status_code == 200 and response.headers.get("Content-Type", "").startswith("image"):
            try:
                tile_path.write_bytes(response.content)
            except OSError as exc:
                logger.error("写入瓦片文件失败 %s: %s", tile_path, exc)
                return None
            logger.info("saved tile %s", tile_path)
            return tile_path

        try:
            data = response.json()
            message = data.get("error", data)
        except ValueError:
            message = response.text

        logger.info(
            "stop at artist=%s work=%s resource=%s x=%s y=%s: status=%s message=%s",
            artist_name,
            work_name,
            child_resource_id,
            x,
            y,
            response.status_code,
            message,
        )
        return None


    def fetch_all_tile(
        self,
        artist_id: str,
        artist_name: str,
        work_id: str,
        work_name: str,
        parent_resource_id: str,
        child_resource_id: str,
        work_src: str,
    ) -> None:
        if self._is_resource_completed(artist_id, work_id, parent_resource_id, child_resource_id):
            logger.info(
                "artist=%s work=%s resource=%s 已完成，跳过下载。",
                artist_name,
                work_name,
                child_resource_id,
            )
            return
        if not self.second_agent_list:
            raise RuntimeError("备用代理列表为空，无法下载切片")

        if ONE_IMAGE_PER_WORK:
            agent_index = self.second_agent_usage % len(self.second_agent_list)
            agent = self.second_agent_list[agent_index]
            self.second_agent_usage += 1

            if (
                USE_PROXY
                and self.second_agent_usage
                and self.second_agent_usage % (len(self.second_agent_list) * 10) == 0
            ):
                self.second_agent_list = self._get_agent_list(self.key, max(self.num * 3, 200))
                if not self.second_agent_list:
                    raise RuntimeError("刷新备用代理失败，列表为空")
                self.second_agent_usage = 0

            result = self.fetch_tile(
                artist_id,
                artist_name,
                work_id,
                work_name,
                parent_resource_id,
                child_resource_id,
                0,
                0,
                agent,
                work_src,
            )

            if result is None:
                logger.warning(
                    "artist=%s work=%s resource=%s 未能成功获取首张切片。",
                    artist_name,
                    work_name,
                    child_resource_id,
                )
                return
            self._mark_resource_completed(artist_id, work_id, parent_resource_id, child_resource_id)
            return

        x = 0
        max_y_limit: Optional[int] = None
        while True:
            y = 0
            any_success = False
            while True:
                if max_y_limit is not None and y >= max_y_limit:
                    break

                agent_index = self.second_agent_usage % len(self.second_agent_list)
                agent = self.second_agent_list[agent_index]
                self.second_agent_usage += 1

                if (
                    USE_PROXY
                    and self.second_agent_usage
                    and self.second_agent_usage % (len(self.second_agent_list) * 10) == 0
                ):
                    self.second_agent_list = self._get_agent_list(self.key, max(self.num * 3, 200))
                    if not self.second_agent_list:
                        raise RuntimeError("刷新备用代理失败，列表为空")
                    self.second_agent_usage = 0

                result = self.fetch_tile(
                    artist_id,
                    artist_name,
                    work_id,
                    work_name,
                    parent_resource_id,
                    child_resource_id,
                    x,
                    y,
                    agent,
                    work_src,
                )
                if result is None:
                    if max_y_limit is None:
                        max_y_limit = y
                    if not any_success:
                        logger.info(
                            "artist=%s work=%s resource=%s 第 %s 列无有效切片，结束。",
                            artist_name,
                            work_name,
                            child_resource_id,
                            x,
                        )
                        self._mark_resource_completed(artist_id, work_id, parent_resource_id, child_resource_id)
                        return
                    break
                any_success = True
                y += 1
            x += 1

        self._mark_resource_completed(artist_id, work_id, parent_resource_id, child_resource_id)

    def for_each_artist(self, index: int, artist_id: str) -> None:
        if USE_PROXY and index % (self.num * 5) == 0:
            self.agent_list = self._get_agent_list(self.key, self.num)
        agent = self.agent_list[index % self.num]

        result = self.get_all_of_artist(artist_id, agent)
        if not result:
            return
        paintings, calligraphies, artist_name = result
        combined = [(work, "SUHA") for work in paintings] + [(work, "SUFA") for work in calligraphies]
        if not combined:
            logger.info("艺术家 %s 无可下载作品", artist_name)
            return

        work_iter = tqdm(combined, desc=f"{artist_name}", unit="work")
        for work, work_src in work_iter:
            work_id = work.get("Id")
            if not work_id:
                logger.warning("艺术家 %s 的作品条目缺少 Id: %s", artist_id, work)
                continue
            work_name = work.get("name") or work_id

            sub_list, parent_suha, resolved_src = self.get_sub_list(artist_id, work, work_src, agent)
            handled = False
            for sub in sub_list:
                suha = sub.get("suha") if isinstance(sub, dict) else None
                if resolved_src == "SUFA" and not suha:
                    suha = sub.get("sufa") if isinstance(sub, dict) else None
                resource_id = suha.get("Id") if isinstance(suha, dict) else None
                if not resource_id:
                    logger.warning("作品 %s 的子资源缺少 Id: %s", work_id, sub)
                    continue

                resource_name = suha.get("name") or resource_id
                resource_data, variants = self.get_resource(artist_id, work_id, work_src, resource_id, resource_name, agent)
                if not variants:
                    logger.warning(
                        "资源 %s 缺少可用 resourceId，跳过。结构: %s",
                        resource_id,
                        resource_data,
                    )
                    continue

                handled = True
                for child_id, resource_variant_name, work_src in variants:
                    self.fetch_all_tile(
                        artist_id,
                        artist_name,
                        work_id,
                        work_name,
                        resource_id,
                        child_id,
                        work_src,
                    )
                    if ONE_IMAGE_PER_WORK:
                        break

            if not handled:
                fallback_suha = parent_suha if isinstance(parent_suha, dict) else {}
                resource_id = fallback_suha.get("Id") or work_id
                resource_name = fallback_suha.get("name") or work_name
                _, variants = self.get_resource(artist_id, work_id, work_src, resource_id, resource_name, agent)
                for child_id, variant_name, work_src in variants or [(resource_id, resource_name, work_src)]:
                    self.fetch_all_tile(
                        artist_id,
                        artist_name,
                        work_id,
                        work_name,
                        resource_id,
                        child_id,
                        work_src,
                    )
                if ONE_IMAGE_PER_WORK:
                    break

    def download(self) -> None:
        pool = ThreadPoolExecutor(max_workers=self.num)
        tasks = [pool.submit(self.for_each_artist, idx, artist_id) for idx, artist_id in enumerate(self.artists_id)]
        wait(tasks, return_when=ALL_COMPLETED)


def main() -> None:
    num = 1 if not USE_PROXY else 75
    downloader = LTFCDownload(artist_csv=r"data/artists.csv", num=num)
    # downloader.download()
    downloader.for_each_artist(0, "5df8a8c15e3be25e694d7136")


if __name__ == "__main__":
    main()

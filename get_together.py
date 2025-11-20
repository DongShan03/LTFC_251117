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
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple, TypeVar

import coloredlogs
import pandas as pd
import requests
from faker import Faker
from tqdm import tqdm

USE_PROXY = True
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
JSON_RETRY_DELAYS = (1.0, 2.0, 4.0)
TILE_RETRY_DELAYS = (1.0, 2.5, 4.5)

_CAG_HOST = "b49b4d8a45b8f098ba881d98abbb5c892f8b5c98"
_RT_PATTERN = re.compile(r"^(http.*//[^/]*)(/.*\.(jpg|jpeg))\?*(.*)$", re.IGNORECASE)


_BUCKET_MS = 31_536_000_000  # 对应 31536e6
_MULTIPLIER = 31_536_000  # 对应 31536e3
MAX_PROXY_RETRIES = 5

T = TypeVar("T")
KEY = "YOUR_TOKEN_HERE"

class ProxyAuthError(RuntimeError):
    """代理身份校验失败，需要更换 IP。"""


class RateLimitError(RuntimeError):
    """请求过于频繁，需要更换 token。"""


def _is_proxy_auth_error(exc: BaseException) -> bool:
    current: Optional[BaseException] = exc  # type: ignore[assignment]
    while current:
        response = getattr(current, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code in (407, 408):
            return True
        if isinstance(current, OSError) and "407 Proxy Authentication Required" in str(current):
            return True
        cause = getattr(current, "__cause__", None)
        context = getattr(current, "__context__", None)
        current = cause or context
    return False

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

    normalized: Dict[str, str] = {}
    for key, value in cleaned.items():
        if not isinstance(value, str):
            raise ValueError(f"代理地址类型错误: {key} -> {value}")
        if not value.startswith(("http://", "https://")):
            value = f"http://{value}"
        normalized[key] = value
    return normalized


def _request_json(
    method: str,
    url: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    session: Optional[requests.Session] = None,
    **kwargs,
) -> Dict:
    last_error: Optional[Exception] = None
    for attempt, delay in enumerate(JSON_RETRY_DELAYS, start=1):
        try:
            requester = session.request if session else requests.request
            response = requester(method, url, timeout=timeout, **kwargs)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and payload.get("Code") == -11:
                raise RateLimitError("请求过于频繁")
            return payload
        except requests.RequestException as exc:
            last_error = exc
            if _is_proxy_auth_error(exc):
                raise ProxyAuthError("代理认证失败") from exc
            logger.warning("%s %s 失败(%s/%s): %s", method.upper(), url, attempt, len(JSON_RETRY_DELAYS), exc)
            time.sleep(delay)
        except RateLimitError:
            raise
        except ValueError as exc:
            last_error = exc
            logger.warning("%s %s 返回非 JSON(%s/%s): %s", method.upper(), url, attempt, len(JSON_RETRY_DELAYS), exc)
            time.sleep(delay)
    raise RuntimeError(f"{method.upper()} {url} 请求异常: {last_error}") from last_error


def _safe_write_json(path: Path, payload: Dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)
    except OSError as exc:
        logger.error("写入文件失败 %s: %s", path, exc)


@dataclass
class SessionBundle:
    session: requests.Session
    tour_token: str


class LTFCDownload:
    def __init__(self, artist_csv: str, num: int = 75):
        self.artist_csv = artist_csv
        self.artists_info = pd.read_csv(self.artist_csv)
        self.artists_id = self.artists_info["Id"].tolist()
        self.num = max(1, min(num, 200))
        self.secondary_usage = 0

        if USE_PROXY:
            self.key = KEY
            # 从环境变量中获取代理密钥
            if self.key == "YOUR_TOKEN_HERE":
                self.key = os.getenv("QINGGOU_KEY", None)
                if not self.key:
                    raise ValueError("Proxy key 未配置，请设置环境变量 QINGGOU_KEY")
            self.token_pool_capacity = max(3, min(self.num * 2, 20))
            self.token_pool: List[str] = []
            self.primary_sessions = self._build_session_pool(self.key, self.num)
            shared_token = self.primary_sessions[0].tour_token if self.primary_sessions else None
            self.secondary_sessions = self._build_session_pool(self.key, min(self.num * 3, 200), shared_token=shared_token)
        else:
            self.key = None
            bundle = self._create_session_bundle(None)
            self.primary_sessions = [bundle]
            self.secondary_sessions = [bundle]
            self.token_pool_capacity = 0
            self.token_pool = []

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

    def _artist_flag_path(self, artist_id: str) -> Path:
        return RAWDATA_DIR / artist_id / ".completed"

    def _is_artist_completed(self, artist_id: str) -> bool:
        return self._artist_flag_path(artist_id).exists()

    def _mark_artist_completed(self, artist_id: str) -> None:
        flag_path = self._artist_flag_path(artist_id)
        try:
            flag_path.parent.mkdir(parents=True, exist_ok=True)
            flag_path.write_text(str(int(time.time())), encoding="utf-8")
        except OSError as exc:
            logger.warning("写入艺术家完成标记失败 %s: %s", flag_path, exc)

    def _mark_resource_completed(self, artist_id: str, work_id: str, parent_resource_id: str, child_resource_id: str) -> None:
        flag_path = self._resource_flag_path(artist_id, work_id, parent_resource_id, child_resource_id)
        try:
            flag_path.parent.mkdir(parents=True, exist_ok=True)
            flag_path.write_text(str(int(time.time())), encoding="utf-8")
        except OSError as exc:
            logger.warning("写入完成标记失败 %s: %s", flag_path, exc)

    def _fetch_proxy_hosts(self, key: str, num: int) -> List[Dict[str, str]]:
        proxy_url = f"https://proxy.qg.net/allocate?Key={key}&Num={num}"
        payload = _request_json("get", proxy_url, timeout=DEFAULT_TIMEOUT)
        data = payload.get("Data") if isinstance(payload, dict) else None
        if not data:
            raise RuntimeError(f"代理服务未返回可用代理: {payload}")

        proxies: List[Dict[str, str]] = []
        for entry in data:
            host = entry.get("host") if isinstance(entry, dict) else None
            try:
                if isinstance(host, dict):
                        proxies.append(_normalize_proxy(host))
                elif isinstance(host, str):
                        proxies.append(_normalize_proxy({"http": host, "https": host}))
                else:
                    logger.warning("跳过无法识别的代理条目: %s", entry)
            except Exception as exc:
                logger.warning("处理代理条目失败 %s: %s", entry, exc)
        if not proxies:
            raise RuntimeError("无法获取任何有效代理，请检查代理服务是否正常")
        return proxies

    def _fetch_tour_token(self, session: requests.Session) -> str:
        payload = _request_json("post", ACCESS_TOKEN_URL, session=session, timeout=DEFAULT_TIMEOUT)
        token = payload.get("token") if isinstance(payload, dict) else None
        if not token:
            raise RuntimeError(f"响应中缺少 token 字段: {payload}")
        return token

    def _create_session_bundle(
        self,
        proxy: Optional[Dict[str, str]],
        *,
        tour_token: Optional[str] = None,
    ) -> SessionBundle:
        session = requests.Session()
        if proxy:
            session.proxies.update(proxy)
        session.headers.update(
            {
                "accept": "application/json",
                "accept-language": "zh-CN,zh;q=0.9",
                "content-type": "application/json;charset=UTF-8",
                "origin": "https://g2.ltfc.net",
                "referer": "https://g2.ltfc.net/",
                "user-agent": ua.user_agent(),
            }
        )
        token = tour_token or self._fetch_tour_token(session)
        return SessionBundle(session=session, tour_token=token)

    def _build_session_pool(
        self,
        key: str,
        num: int,
        *,
        shared_token: Optional[str] = None,
    ) -> List[SessionBundle]:
        target_count = max(1, min(num, 200))
        bundles: List[SessionBundle] = []
        attempts = 0

        while attempts < MAX_PROXY_RETRIES and len(bundles) < target_count:
            proxies = self._fetch_proxy_hosts(key, target_count)
            token_cache = shared_token
            for proxy in proxies:
                try:
                    bundle = self._create_session_bundle(proxy, tour_token=token_cache)
                    token_cache = bundle.tour_token
                    self._push_token(bundle.tour_token)
                    bundles.append(bundle)
                    if len(bundles) >= target_count:
                        break
                except ProxyAuthError as exc:
                    logger.warning("代理 %s 认证失败，尝试更换 IP: %s", proxy, exc)
                    continue
                except Exception as exc:
                    logger.warning("创建会话失败 %s: %s", proxy, exc)
            if bundles:
                break
            attempts += 1
            time.sleep(1)

        if not bundles:
            raise RuntimeError("无法构建会话池，请检查代理服务是否正常")
        return bundles

    def _refresh_secondary_sessions(self, *, force_new_token: bool) -> SessionBundle:
        if not USE_PROXY:
            if not self.secondary_sessions:
                raise RuntimeError("备用会话列表为空，无法刷新")
            return self.secondary_sessions[0]
        if not self.key:
            raise RuntimeError("代理 key 未配置，无法刷新备用会话")
        primary_token = self.primary_sessions[0].tour_token if self.primary_sessions else None
        shared_token = None if force_new_token else primary_token
        self.secondary_sessions = self._build_session_pool(self.key, min(self.num * 3, 200), shared_token=shared_token)
        self.secondary_usage = 0
        if not self.secondary_sessions:
            raise RuntimeError("刷新备用会话失败，列表为空")
        return self.secondary_sessions[0]

    def _acquire_secondary_session(self, *, force_new_token: bool) -> SessionBundle:
        if not USE_PROXY:
            if not self.secondary_sessions:
                raise RuntimeError("备用会话列表为空，无法获取新的会话")
            return self.secondary_sessions[0]
        if not self.key:
            raise RuntimeError("代理 key 未配置，无法获取新的备用会话")
        primary_token = self.primary_sessions[0].tour_token if self.primary_sessions else None
        shared_token = None if force_new_token else primary_token
        attempts = 0
        while attempts < MAX_PROXY_RETRIES:
            proxies = self._fetch_proxy_hosts(self.key, 1)
            token_cache = shared_token
            for proxy in proxies:
                try:
                    bundle = self._create_session_bundle(proxy, tour_token=token_cache)
                    self._push_token(bundle.tour_token)
                    return bundle
                except ProxyAuthError as exc:
                    logger.warning("新备用会话代理 %s 认证失败: %s", proxy, exc)
                    continue
                except Exception as exc:
                    logger.warning("新备用会话创建失败 %s: %s", proxy, exc)
            attempts += 1
            time.sleep(1)
        raise RuntimeError("无法获取新的备用会话，请检查代理服务")

    def _replace_secondary_session(self, index: int, *, force_new_token: bool) -> SessionBundle:
        if index < 0 or index >= len(self.secondary_sessions):
            raise RuntimeError(f"备用会话索引越界: {index}")
        bundle = self._acquire_secondary_session(force_new_token=force_new_token)
        self.secondary_sessions[index] = bundle
        return bundle

    def _find_primary_index(self, bundle: SessionBundle) -> Optional[int]:
        for idx, candidate in enumerate(self.primary_sessions):
            if candidate is bundle:
                return idx
        return None

    def _acquire_primary_session(self, *, force_new_token: bool) -> SessionBundle:
        if not USE_PROXY:
            if not self.primary_sessions:
                raise RuntimeError("主会话列表为空，无法获取新的会话")
            return self.primary_sessions[0]
        if not self.key:
            raise RuntimeError("代理 key 未配置，无法获取新的主会话")
        attempts = 0
        while attempts < MAX_PROXY_RETRIES:
            proxies = self._fetch_proxy_hosts(self.key, 1)
            for proxy in proxies:
                try:
                    bundle = self._create_session_bundle(proxy)
                    self._push_token(bundle.tour_token)
                    return bundle
                except ProxyAuthError as exc:
                    logger.warning("新主会话代理 %s 认证失败: %s", proxy, exc)
                    continue
                except Exception as exc:
                    logger.warning("新主会话创建失败 %s: %s", proxy, exc)
            attempts += 1
            time.sleep(1)
        raise RuntimeError("无法获取新的主会话，请检查代理服务")

    def _replace_primary_session(
        self,
        current_bundle: SessionBundle,
        *,
        index: Optional[int],
        force_new_token: bool,
    ) -> Tuple[SessionBundle, int]:
        if not self.primary_sessions:
            new_bundle = self._acquire_primary_session(force_new_token=force_new_token)
            self.primary_sessions = [new_bundle]
            return new_bundle, 0
        actual_index = index if index is not None else self._find_primary_index(current_bundle)
        if actual_index is None:
            actual_index = 0
        actual_index %= len(self.primary_sessions)
        new_bundle = self._acquire_primary_session(force_new_token=force_new_token)
        self.primary_sessions[actual_index] = new_bundle
        return new_bundle, actual_index

    def _warm_up_token_pool(self) -> None:
        if not USE_PROXY or not self.primary_sessions or self.token_pool_capacity <= 0:
            return
        base_session = self.primary_sessions[0].session
        attempts = 0
        while len(self.token_pool) < self.token_pool_capacity and attempts < self.token_pool_capacity * 3:
            attempts += 1
            try:
                token = self._fetch_tour_token(base_session)
                if token:
                    self._push_token(token)
                time.sleep(0.2)
            except RateLimitError as exc:
                logger.info("预生成 token 时频率限制: %s", exc)
                time.sleep(1)
            except Exception as exc:
                logger.warning("预生成 token 失败: %s", exc)
                break

    def _acquire_token(self, base_session: requests.Session, *, force_new: bool = False) -> str:
        if not force_new and self.token_pool:
            return self.token_pool.pop()
        attempts = 0
        last_error: Optional[Exception] = None
        while attempts < MAX_PROXY_RETRIES:
            attempts += 1
            try:
                return self._fetch_tour_token(base_session)
            except RateLimitError as exc:
                last_error = exc
                logger.info("获取 token 频率受限，等待重试(%s/%s)...", attempts, MAX_PROXY_RETRIES)
                time.sleep(1)
            except Exception as exc:
                last_error = exc
                logger.warning("获取 token 失败(%s/%s): %s", attempts, MAX_PROXY_RETRIES, exc)
                time.sleep(1)
        raise RuntimeError("获取 token 失败") from last_error

    def _maybe_replenish_token_pool(self, base_session: requests.Session) -> None:
        if not USE_PROXY or self.token_pool_capacity <= 0:
            return
        if len(self.token_pool) >= self.token_pool_capacity:
            return
        try:
            token = self._fetch_tour_token(base_session)
            if token:
                self._push_token(token)
        except RateLimitError:
            logger.debug("补充 token 时仍受限，稍后重试")
        except Exception as exc:
            logger.debug("补充 token 失败: %s", exc)

    def _push_token(self, token: Optional[str]) -> None:
        if not token or self.token_pool_capacity <= 0:
            return
        if len(self.token_pool) >= self.token_pool_capacity:
            self.token_pool.pop(0)
        if token not in self.token_pool:
            self.token_pool.append(token)

    def _discard_token(self, token: Optional[str]) -> None:
        if not token:
            return
        try:
            self.token_pool.remove(token)
        except ValueError:
            pass

    def _rotate_token_for_bundle(
        self,
        bundle: SessionBundle,
        index: Optional[int],
        *,
        force_new: bool = False,
        old_token: Optional[str] = None,
    ) -> Tuple[SessionBundle, Optional[int]]:
        if old_token:
            self._discard_token(old_token)
        actual_index = index if index is not None else self._find_primary_index(bundle)
        new_token = self._acquire_token(bundle.session, force_new=force_new)
        bundle.tour_token = new_token
        if actual_index is not None and actual_index < len(self.primary_sessions):
            self.primary_sessions[actual_index].tour_token = new_token
        self._maybe_replenish_token_pool(bundle.session)
        return bundle, actual_index

    def _request_with_bundle(
        self,
        bundle: SessionBundle,
        url: str,
        payload: Dict,
        *,
        pool: str = "primary",
        bundle_index: Optional[int] = None,
    ) -> Tuple[Dict, SessionBundle, Optional[int]]:
        def _task(active_bundle: SessionBundle) -> Dict:
            return _request_json(
                "post",
                url,
                json=payload,
                session=active_bundle.session,
                timeout=DEFAULT_TIMEOUT,
            )

        return self._with_proxy_retry(bundle, pool, _task, bundle_index=bundle_index)

    def _get_primary_bundle(self, index: int) -> Tuple[SessionBundle, int]:
        if not self.primary_sessions:
            raise RuntimeError("主会话列表为空，无法处理艺术家")
        if not USE_PROXY:
            bundle_index = index % len(self.primary_sessions)
            return self.primary_sessions[bundle_index], bundle_index
        if not self.key:
            raise RuntimeError("代理 key 未配置，无法刷新主会话")
        if index % max(1, self.num) == 0:
            self.primary_sessions = self._build_session_pool(self.key, self.num)
            if self.token_pool_capacity > 0:
                self._warm_up_token_pool()
        if not self.primary_sessions:
            raise RuntimeError("刷新主会话失败，列表为空")
        bundle_index = index % len(self.primary_sessions)
        return self.primary_sessions[bundle_index], bundle_index

    def _with_proxy_retry(
        self,
        bundle: SessionBundle,
        pool: str,
        operation: Callable[[SessionBundle], T],
        *,
        bundle_index: Optional[int] = None,
    ) -> Tuple[T, SessionBundle, Optional[int]]:
        if not USE_PROXY:
            return operation(bundle), bundle, bundle_index

        attempts = 0
        rate_limit_attempts = 0
        current_bundle = bundle
        current_index = bundle_index
        while True:
            try:
                return operation(current_bundle), current_bundle, current_index
            except RateLimitError as exc:
                rate_limit_attempts += 1
                if rate_limit_attempts >= MAX_PROXY_RETRIES:
                    raise RuntimeError("请求过于频繁，多次刷新 token 仍失败") from exc
                logger.info("检测到请求过于频繁(%s)，轮换 token 后重试(%s/%s)...", exc, rate_limit_attempts, MAX_PROXY_RETRIES)
                previous_token = current_bundle.tour_token
                current_bundle, current_index = self._rotate_token_for_bundle(
                    current_bundle,
                    current_index,
                    force_new=(not self.token_pool),
                    old_token=previous_token,
                )
                continue
            except ProxyAuthError as exc:
                attempts += 1
                if not self.key or attempts >= MAX_PROXY_RETRIES:
                    raise RuntimeError("代理认证多次失败，请检查代理服务") from exc
                if pool == "primary":
                    current_bundle, current_index = self._replace_primary_session(
                        current_bundle,
                        index=current_index,
                        force_new_token=True,
                    )
                else:
                    if current_index is None:
                        current_bundle = self._refresh_secondary_sessions(force_new_token=True)
                        current_index = 0
                    else:
                        current_bundle = self._replace_secondary_session(current_index, force_new_token=True)

    def _next_secondary_bundle(self) -> Tuple[SessionBundle, int]:
        if not self.secondary_sessions:
            bundle = self._refresh_secondary_sessions(force_new_token=True)
            return bundle, 0
        index = self.secondary_usage % len(self.secondary_sessions)
        bundle = self.secondary_sessions[index]
        self.secondary_usage += 1
        return bundle, index

    def _fetch_artist_resources(
        self,
        url: str,
        artist_id: str,
        bundle: SessionBundle,
        bundle_index: Optional[int],
    ) -> Tuple[Dict, SessionBundle, Optional[int]]:
        json_data = {
            "Id": artist_id,
            "page": {"skip": 0, "limit": 999},
            "context": {"tourToken": bundle.tour_token},
        }
        write_file = RAWDATA_DIR / artist_id / ("all_huia_of_artist.json" if url == ALL_HUIA_OF_ARTIST_URL else "all_sufa_of_artist.json")
        try:
            payload, active_bundle, bundle_index = self._request_with_bundle(
                bundle,
                url,
                json_data,
                pool="primary",
                bundle_index=bundle_index,
            )
        except RuntimeError as exc:
            logger.error("访问 %s 失败: %s", url, exc)
            _safe_write_json(write_file, {"error": str(exc), "request": json_data})
            return {"data": []}, bundle, bundle_index

        _safe_write_json(write_file, payload)
        return (payload if isinstance(payload, dict) else {"data": []}, active_bundle, bundle_index)

    def get_all_of_artist(
        self,
        artist_id: str,
        bundle: SessionBundle,
        bundle_index: Optional[int],
    ) -> tuple[List[Dict], List[Dict], str, SessionBundle, Optional[int]]:
        payload_painting, bundle, bundle_index = self._fetch_artist_resources(ALL_HUIA_OF_ARTIST_URL, artist_id, bundle, bundle_index)
        payload_calligraphy, bundle, bundle_index = self._fetch_artist_resources(ALL_SUFA_OF_ARTIST_URL, artist_id, bundle, bundle_index)

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
        return paint_data, sufa_data, artist_name, bundle, bundle_index

    def get_sub_list(
        self,
        artist_id: str,
        work: Dict,
        work_src: str,
        bundle: SessionBundle,
        bundle_index: Optional[int],
    ) -> tuple[List[Dict], Optional[Dict], str, SessionBundle, Optional[int]]:
        work_id = work.get("Id") or ""
        json_data = {
            "src": work_src,
            "id": work_id,
            "context": {"tourToken": bundle.tour_token},
        }
        write_file = RAWDATA_DIR / artist_id / work_id / "sub_list.json"
        try:
            payload, active_bundle, bundle_index = self._request_with_bundle(
                bundle,
                SUB_LIST_URL,
                json_data,
                pool="primary",
                bundle_index=bundle_index,
            )
        except RuntimeError as exc:
            logger.error("获取艺术家 %s 作品 %s 的子资源列表失败: %s", artist_id, work_id, exc)
            _safe_write_json(write_file, {"error": str(exc), "request": json_data})
            return [], None, work_src, bundle, bundle_index

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
        return data, parent_suha, work_src, active_bundle, bundle_index

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

    def get_resource(
        self,
        artist_id: str,
        work_id: str,
        work_src: str,
        resource_id: str,
        resource_name: str,
        bundle: SessionBundle,
        bundle_index: Optional[int],
    ) -> tuple[Dict, List[tuple[str, str]], SessionBundle, Optional[int]]:
        json_data = {
            "id": resource_id,
            "src": work_src,
            "context": {"tourToken": bundle.tour_token},
        }
        parent_root = self._resource_root(artist_id, work_id, resource_id)
        try:
            payload, active_bundle, bundle_index = self._request_with_bundle(
                bundle,
                RESOURCE_ID_URL,
                json_data,
                pool="primary",
                bundle_index=bundle_index,
            )
        except RuntimeError as exc:
            logger.error("获取资源 %s 详情失败: %s", resource_id, exc)
            return {}, [], bundle, bundle_index

        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            logger.warning("资源 %s 详情数据异常: %s", resource_id, payload)
            return {}, [], bundle, bundle_index
        _safe_write_json(parent_root / "resource.json", payload)
        variants = self._extract_resource_variants(data, work_src)
        if not variants:
            variants = [(resource_id, resource_name, work_src)]
        return data, variants, active_bundle, bundle_index

    def _current_bucket_hex(self) -> str:
        now_ms = int(time.time() * 1000)
        value = math.ceil(now_ms / _BUCKET_MS) * _MULTIPLIER
        return format(value, "x")

    def get_SUFA_detail_url(self, url: Optional[str]) -> Optional[str]:
        result = subprocess.run(["node", r"utils/get_USFA.js", "init", url.replace("cag.ltfc.net", "cag-ac.ltfc.net")], capture_output=True, text=True)
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
        bundle: SessionBundle,
        bundle_index: int,
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

        retry_schedule = TILE_RETRY_DELAYS if USE_PROXY else TILE_RETRY_DELAYS[:1]
        attempt = 0
        current_bundle = bundle
        current_index = bundle_index
        replacement_attempts = 0
        while attempt < len(retry_schedule):
            delay = retry_schedule[attempt]
            try:
                response = current_bundle.session.get(url, timeout=DEFAULT_TIMEOUT)
            except requests.RequestException as exc:
                if USE_PROXY and self.key and _is_proxy_auth_error(exc):
                    replacement_attempts += 1
                    if replacement_attempts >= MAX_PROXY_RETRIES:
                        logger.error(
                            "备用会话多次认证失败 artist=%s work=%s resource=%s (%s,%s)",
                            artist_name,
                            work_name,
                            child_resource_id,
                            x,
                            y,
                        )
                        break
                    current_bundle = self._replace_secondary_session(current_index, force_new_token=True)
                    continue
                logger.warning(
                    "下载瓦片失败 artist=%s(%s) work=%s(%s) resource=%s (%s,%s) attempt=%s/%s: %s",
                    artist_name,
                    artist_id,
                    work_name,
                    work_id,
                    child_resource_id,
                    x,
                    y,
                    attempt + 1,
                    len(retry_schedule),
                    exc,
                )
                time.sleep(delay)
                attempt += 1
                continue

            if response.status_code in (407, 408) and USE_PROXY and self.key:
                replacement_attempts += 1
                if replacement_attempts >= MAX_PROXY_RETRIES:
                    logger.error(
                        "备用会话多次返回 %s artist=%s work=%s resource=%s (%s,%s)",
                        response.status_code,
                        artist_name,
                        work_name,
                        child_resource_id,
                        x,
                        y,
                    )
                    break
                force_new_token = response.status_code == 407
                current_bundle = self._replace_secondary_session(current_index, force_new_token=force_new_token)
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

            logger.warning(
                "下载瓦片失败 artist=%s work=%s resource=%s x=%s y=%s: status=%s message=%s (%s/%s)",
                artist_name,
                work_name,
                child_resource_id,
                x,
                y,
                response.status_code,
                message,
                attempt + 1,
                len(retry_schedule),
            )
            time.sleep(delay)
            attempt += 1
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
    ) -> bool:
        if self._is_resource_completed(artist_id, work_id, parent_resource_id, child_resource_id):
            logger.info(
                "artist=%s work=%s resource=%s 已完成，跳过下载。",
                artist_name,
                work_name,
                child_resource_id,
            )
            return True
        if not self.secondary_sessions:
            raise RuntimeError("备用会话列表为空，无法下载切片")

        x = 0
        max_y_limit: Optional[int] = None
        consecutive_empty_columns = 0
        any_tile_downloaded = False
        while True:
            y = 0
            column_success = False
            current_column = x
            while True:
                if max_y_limit is not None and y >= max_y_limit:
                    break

                bundle, bundle_index = self._next_secondary_bundle()

                result = self.fetch_tile(
                    artist_id,
                    artist_name,
                    work_id,
                    work_name,
                    parent_resource_id,
                    child_resource_id,
                    x,
                    y,
                    bundle,
                    bundle_index,
                    work_src,
                )
                if result is None:
                    if max_y_limit is None:
                        max_y_limit = y
                    break

                column_success = True
                any_tile_downloaded = True
                y += 1

            if column_success:
                consecutive_empty_columns = 0
            else:
                consecutive_empty_columns += 1
                logger.info(
                    "artist=%s work=%s resource=%s 列 %s 无有效切片，连续空列=%s",
                    artist_name,
                    work_name,
                    child_resource_id,
                    current_column,
                    consecutive_empty_columns,
                )
                if consecutive_empty_columns >= 3:
                    if any_tile_downloaded:
                        self._mark_resource_completed(artist_id, work_id, parent_resource_id, child_resource_id)
                    else:
                        logger.warning(
                            "artist=%s work=%s resource=%s 连续 %s 列无有效切片，未写入完成标记",
                            artist_name,
                            work_name,
                            child_resource_id,
                            consecutive_empty_columns,
                        )
                    return any_tile_downloaded

            x += 1

        return any_tile_downloaded

    def for_each_artist(self, index: int, artist_id: str) -> None:
        if self._is_artist_completed(artist_id):
            logger.info("艺术家 %s 已完成，跳过。", artist_id)
            return

        bundle, bundle_index = self._get_primary_bundle(index)

        paintings, calligraphies, artist_name, bundle, bundle_index = self.get_all_of_artist(artist_id, bundle, bundle_index)
        combined = [(work, "SUHA") for work in paintings] + [(work, "SUFA") for work in calligraphies]
        if not combined:
            logger.info("艺术家 %s 无可下载作品", artist_name)
            return

        work_iter = tqdm(combined, desc=f"{artist_name}", unit="work")
        artist_completed = False
        for work, work_src in work_iter:
            work_downloaded = False
            work_id = work.get("Id")
            if not work_id:
                logger.warning("艺术家 %s 的作品条目缺少 Id: %s", artist_id, work)
                continue
            work_name = work.get("name") or work_id

            sub_list, parent_suha, resolved_src, bundle, bundle_index = self.get_sub_list(
                artist_id,
                work,
                work_src,
                bundle,
                bundle_index,
            )
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
                resource_data, variants, bundle, bundle_index = self.get_resource(
                    artist_id,
                    work_id,
                    work_src,
                    resource_id,
                    resource_name,
                    bundle,
                    bundle_index,
                )
                if not variants:
                    logger.warning(
                        "资源 %s 缺少可用 resourceId，跳过。结构: %s",
                        resource_id,
                        resource_data,
                    )
                    continue

                handled = True
                for child_id, resource_variant_name, work_src in variants:
                    success = self.fetch_all_tile(
                        artist_id,
                        artist_name,
                        work_id,
                        work_name,
                        resource_id,
                        child_id,
                        work_src,
                    )
                    if success:
                        work_downloaded = True
                    if ONE_IMAGE_PER_WORK:
                        break

            if not handled:
                fallback_suha = parent_suha if isinstance(parent_suha, dict) else {}
                resource_id = fallback_suha.get("Id") or work_id
                resource_name = fallback_suha.get("name") or work_name
                _, variants, bundle, bundle_index = self.get_resource(
                    artist_id,
                    work_id,
                    work_src,
                    resource_id,
                    resource_name,
                    bundle,
                    bundle_index,
                )
                for child_id, variant_name, work_src in variants or [(resource_id, resource_name, work_src)]:
                    success = self.fetch_all_tile(
                        artist_id,
                        artist_name,
                        work_id,
                        work_name,
                        resource_id,
                        child_id,
                        work_src,
                    )
                    if success:
                        work_downloaded = True
                if ONE_IMAGE_PER_WORK:
                    break

            if handled:
                work_downloaded = True

            if work_downloaded:
                artist_completed = True

        if artist_completed:
            self._mark_artist_completed(artist_id)

    def download(self) -> None:
        pool = ThreadPoolExecutor(max_workers=self.num)
        tasks = [pool.submit(self.for_each_artist, idx, artist_id) for idx, artist_id in enumerate(self.artists_id)]
        wait(tasks, return_when=ALL_COMPLETED)


def main() -> None:
    # num = 1 if not USE_PROXY else 5
    num = 1 if not USE_PROXY else 10
    downloader = LTFCDownload(artist_csv=r"data/artists.csv", num=num)
    downloader.download()
    # downloader.for_each_artist(0, "5df8a8c15e3be25e694d7134")


if __name__ == "__main__":
    main()

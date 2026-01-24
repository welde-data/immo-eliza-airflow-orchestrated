from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Iterable, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass(frozen=True)
class HttpConfig:
    timeout_s: float = 20.0
    max_retries: int = 5
    backoff_factor: float = 0.6
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504)
    user_agents: tuple[str, ...] = (
        # Keep simple and realistic; avoid anti-detection tricks.
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    )


def build_session(cfg: HttpConfig, extra_headers: Optional[dict[str, str]] = None) -> requests.Session:
    s = requests.Session()

    retry = Retry(
        total=cfg.max_retries,
        connect=cfg.max_retries,
        read=cfg.max_retries,
        status=cfg.max_retries,
        backoff_factor=cfg.backoff_factor,
        status_forcelist=cfg.status_forcelist,
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)

    headers = {
        "User-Agent": random.choice(cfg.user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    if extra_headers:
        headers.update(extra_headers)

    s.headers.update(headers)
    return s


def polite_sleep(min_s: float = 0.3, max_s: float = 1.0) -> None:
    time.sleep(random.uniform(min_s, max_s))


def fetch_html(session: requests.Session, url: str, timeout_s: float) -> tuple[int, str]:
    resp = session.get(url, timeout=timeout_s)
    return resp.status_code, resp.text

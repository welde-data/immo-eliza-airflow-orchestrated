"""
Property Scraper - Working-base + targeted fixes

- heating_type no longer becomes numeric (1, 90, etc.)
- cadastral_income_eur extracted via (1) h4 mapping, (2) regex scan fallback
- swimming_pool stays "Yes"/"No"/NA
- locality_name is city name without postal code
- fixed output schema
"""

from __future__ import annotations

import os
import re
import random
from time import sleep
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent


# ------------------ helpers ------------------

def _clean(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    return re.sub(r"\s+", " ", s).strip() or None


def _txt(el) -> Optional[str]:
    return _clean(el.get_text(" ", strip=True)) if el else None


def _first_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    s = s.replace("\u202f", " ").replace("\xa0", " ")
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else None


def _price_eur(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    s = _clean(s.replace("\u202f", " ").replace("\xa0", " "))
    m = re.search(r"(\d[\d\s\.]*)\s*€", s)
    if not m:
        return None
    digits = re.sub(r"[^\d]", "", m.group(1))
    return int(digits) if digits else None


def _m2(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    s = s.replace("\u202f", " ").replace("\xa0", " ")
    m = re.search(r"(\d+)\s*m²", s)
    return int(m.group(1)) if m else None


def _epc(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"(\d+)\s*kWh\/m²\/year", s)
    return int(m.group(1)) if m else None


def _yes_no_na(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    v = s.strip().lower()
    if "yes" in v or v in ("true", "y"):
        return "Yes"
    if "no" in v or v in ("false", "n"):
        return "No"
    return None


def _postal_locality(locality_raw: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not locality_raw:
        return None, None
    locality_raw = _clean(locality_raw)
    m = re.match(r"^\s*(\d{4})\s+(.*)$", locality_raw or "")
    if not m:
        name = re.sub(r"^\s*\d{4}\s*[-–]?\s*", "", locality_raw or "").strip() or None
        return None, name
    return m.group(1), (m.group(2).strip() or None)


def _norm_label(label: str) -> str:
    label = _clean(label) or ""
    label = label.lower().replace(":", "")
    label = re.sub(r"[^\w\s\-\(\)²/]+", "", label)
    label = label.replace("²", "2")
    return re.sub(r"\s+", " ", label).strip()


def _property_type_from_url(url: str) -> Optional[str]:
    m = re.search(r"/en/detail/([^/]+)/", url)
    if not m:
        return None
    raw = m.group(1).strip().lower()
    mapping = {
        "apartment": "apartment",
        "studio": "studio",
        "flat-studio": "studio",
        "loft": "loft",
        "house": "house",
        "residence": "house",
        "villa": "house",
        "master-house": "house",
        "bungalow": "bungalow",
        "mixed-building": "mixed-building",
        "duplex": "duplex",
    }
    return mapping.get(raw, raw)


def _looks_like_locality(v: str) -> bool:
    return re.match(r"^\s*\d{4}\s+[A-Za-z].+$", v or "") is not None


# ------------------ mapping ------------------

LABEL_TO_KEY = {
    "state of the property": "state_of_the_property",
    "condition": "state_of_the_property",

    "currently leased": "currently_leased",

    "build year": "build_year",
    "year of construction": "build_year",

    "number of bedrooms": "bedrooms",
    "number of bathrooms": "bathrooms",
    "number of toilets": "toilets",

    "livable surface": "livable_surface_m2",
    "living area": "livable_surface_m2",
    "habitable surface": "livable_surface_m2",

    "total land surface": "total_land_surface_m2",
    "surface terrace": "terrace_m2",
    "surface garden": "garden_m2",

    "type of heating": "heating_type",

    "specific primary energy consumption": "epc_kwh_m2_year",
    "epc": "epc_kwh_m2_year",
    "peb": "epc_kwh_m2_year",

    "swimming pool": "swimming_pool",

    # cadastral variants
    "cadastral income": "cadastral_income_eur",
    "cadastral revenue": "cadastral_income_eur",
    "cadastral income (not indexed)": "cadastral_income_eur",
    "cadastral income (non-indexed)": "cadastral_income_eur",
}


def _validate_value(key: str, value: str) -> bool:
    value = _clean(value) or ""
    if not value:
        return False

    if key in ("bedrooms", "bathrooms", "toilets", "build_year"):
        return _first_int(value) is not None

    if key in ("livable_surface_m2", "total_land_surface_m2", "terrace_m2", "garden_m2"):
        return _m2(value) is not None

    if key == "epc_kwh_m2_year":
        return _epc(value) is not None

    if key in ("currently_leased", "swimming_pool"):
        return _yes_no_na(value) is not None

    if key == "state_of_the_property":
        if len(value) > 60:
            return False
        if _looks_like_locality(value):
            return False
        return True

    if key == "heating_type":
        # must contain letters (prevents 1, 90)
        if not re.search(r"[A-Za-z]", value):
            return False
        # exclude values that look like just units
        if re.fullmatch(r"\d+\s*(m2|m²)?", value.lower()):
            return False
        return len(value) <= 60

    if key == "cadastral_income_eur":
        return (_price_eur(value) is not None) or (_first_int(value) is not None)

    return True


def _coerce_value(key: str, value: str) -> Any:
    value = _clean(value)
    if value is None:
        return None

    if key in ("bedrooms", "bathrooms", "toilets", "build_year"):
        return _first_int(value)

    if key in ("livable_surface_m2", "total_land_surface_m2", "terrace_m2", "garden_m2"):
        return _m2(value)

    if key == "epc_kwh_m2_year":
        return _epc(value)

    if key in ("currently_leased", "swimming_pool"):
        return _yes_no_na(value)

    if key == "cadastral_income_eur":
        return _price_eur(value) or _first_int(value)

    return value


def _find_more_information_container(soup: BeautifulSoup):
    for h in soup.find_all(["h2", "h3", "h4"]):
        t = _txt(h)
        if t and t.strip().lower() == "more information":
            return h.parent or soup
    return soup


def _value_near_h4(h4) -> Optional[str]:
    """
    Critical: avoid DOM-jumping.
    Try a small, local neighborhood in priority order.
    """
    # 1) next sibling element
    sib = h4.find_next_sibling()
    v = _txt(sib) if sib else None
    if v:
        return v

    # 2) parent's next sibling (common in grid layouts)
    parent = h4.parent
    if parent:
        sib2 = parent.find_next_sibling()
        v2 = _txt(sib2) if sib2 else None
        if v2:
            return v2

    # 3) next element but limit to short distance by checking immediate few siblings
    cur = h4
    for _ in range(4):
        cur = cur.find_next()
        vv = _txt(cur) if cur else None
        if vv:
            return vv

    return None


def _extract_from_h4_pairs(container: BeautifulSoup) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    for h4 in container.find_all("h4", class_=False):
        label_raw = _txt(h4)
        if not label_raw:
            continue

        label = _norm_label(label_raw)
        key = LABEL_TO_KEY.get(label)

        if not key:
            # keyword-based cadastral catch
            if "cadastral" in label:
                key = "cadastral_income_eur"
            else:
                continue

        if key in out and out[key] is not None:
            continue

        value = _value_near_h4(h4)
        if not value:
            continue

        # additional guard: state must not be locality-like
        if key == "state_of_the_property" and _looks_like_locality(value):
            continue

        if _validate_value(key, value):
            out[key] = _coerce_value(key, value)

    return out


def _extract_cadastral_by_regex(container: BeautifulSoup) -> Optional[int]:
    """
    Fallback when cadastral is not in h4 labels:
    Scan local block text for "cadastral" + amount.
    """
    text = _clean(container.get_text(" ", strip=True)) or ""
    if "cadastral" not in text.lower():
        return None

    # Example patterns: "Cadastral income 877 €", "Cadastral income (not indexed) 877 €"
    m = re.search(r"cadastral\s+(?:income|revenue)[^0-9]{0,40}(\d[\d\s\.]*)\s*€", text, re.IGNORECASE)
    if m:
        digits = re.sub(r"[^\d]", "", m.group(1))
        return int(digits) if digits else None

    return None


# ------------------ output schema ------------------

OUTPUT_COLUMNS = [
    "property_url",
    "property_code",
    "property_type",
    "price_eur",
    "cadastral_income_eur",
    "postal_code",
    "locality_name",
    "state_of_the_property",
    "currently_leased",
    "build_year",
    "bedrooms",
    "bathrooms",
    "toilets",
    "livable_surface_m2",
    "total_land_surface_m2",
    "terrace_m2",
    "garden_m2",
    "heating_type",
    "epc_kwh_m2_year",
    "swimming_pool",
]


class PropertyScraper:
    def __init__(self, output_dir: str = "/opt/airflow/data/raw") -> None:
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        self.delay_min = float(os.getenv("SCRAPING_DELAY_MIN", "0.8"))
        self.delay_max = float(os.getenv("SCRAPING_DELAY_MAX", "1.8"))
        self.max_retries = int(os.getenv("SCRAPE_MAX_RETRIES", "4"))
        self.backoff_base = float(os.getenv("SCRAPE_BACKOFF_BASE", "1.5"))
        self.timeout_seconds = int(os.getenv("SCRAPE_TIMEOUT_SECONDS", "30"))

        self.urls: List[str] = []
        self.rows: List[Dict[str, Any]] = []

    def load_urls(self, csv_path: str, limit: Optional[int] = None) -> None:
        df = pd.read_csv(csv_path, nrows=limit)
        if "link" in df.columns:
            urls = df["link"].astype(str).tolist()
        elif "url" in df.columns:
            urls = df["url"].astype(str).tolist()
        else:
            urls = df.iloc[:, 0].astype(str).tolist()

        urls = [u.strip() for u in urls if isinstance(u, str) and u.strip()]
        urls = [u for u in urls if "/en/detail/" in u]
        self.urls = list(dict.fromkeys(urls))
        print(f"✅ Loaded {len(self.urls)} unique DETAIL URLs")

    def _session(self) -> requests.Session:
        ua = UserAgent()
        s = requests.Session()
        s.headers.update(
            {
                "User-Agent": ua.random,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )
        return s

    def _sleep(self) -> None:
        sleep(random.uniform(self.delay_min, self.delay_max))

    def _fetch(self, url: str, s: requests.Session) -> Optional[BeautifulSoup]:
        for attempt in range(1, self.max_retries + 1):
            self._sleep()
            try:
                r = s.get(url, timeout=self.timeout_seconds)
                if r.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"retryable_status={r.status_code}")
                r.raise_for_status()
                return BeautifulSoup(r.content, "html.parser")
            except Exception:
                if attempt == self.max_retries:
                    return None
                sleep((self.backoff_base ** attempt) + random.uniform(0, 1))
        return None

    def run(self, limit: Optional[int] = None) -> None:
        s = self._session()
        urls = self.urls[:limit] if limit else self.urls
        total = len(urls)
        print(f"=== Scraping {total} URLs ===")

        for idx, url in enumerate(urls, 1):
            soup = self._fetch(url, s)
            if soup is None:
                self.rows.append({"property_url": url, "scraping_error": True})
                continue

            locality_raw = _txt(soup.find(class_="city-line"))
            postal_code, locality_name = _postal_locality(locality_raw)

            row: Dict[str, Any] = {
                "property_url": url,
                "property_code": _txt(soup.find(class_="vlancode")),
                "property_type": _property_type_from_url(url),
                "price_eur": _price_eur(_txt(soup.find(class_="detail__header_price_data"))),
                "postal_code": postal_code,
                "locality_name": locality_name,
            }

            container = _find_more_information_container(soup)
            feats = _extract_from_h4_pairs(container)
            if not feats:
                feats = _extract_from_h4_pairs(soup)

            # Cadastral fallback by regex if still missing
            if feats.get("cadastral_income_eur") is None:
                cad = _extract_cadastral_by_regex(container)
                if cad is None:
                    cad = _extract_cadastral_by_regex(soup)
                if cad is not None:
                    feats["cadastral_income_eur"] = cad

            row.update(feats)
            self.rows.append(row)

            if idx % 25 == 0:
                print(f"  ...{idx}/{total}")

    def save(self, filename: str = "properties_raw.csv") -> str:
        path = os.path.join(self.output_dir, filename)
        df = pd.json_normalize(self.rows)

        if "property_url" in df.columns:
            df = df.drop_duplicates(subset=["property_url"])

        for c in OUTPUT_COLUMNS:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[OUTPUT_COLUMNS]

        for c in [
            "price_eur",
            "cadastral_income_eur",
            "build_year",
            "bedrooms",
            "bathrooms",
            "toilets",
            "livable_surface_m2",
            "terrace_m2",
            "garden_m2",
            "epc_kwh_m2_year",
            "total_land_surface_m2",
        ]:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

        df["postal_code"] = df["postal_code"].astype("string")
        df["locality_name"] = df["locality_name"].astype("string")

        for c in ["currently_leased", "swimming_pool"]:
            df[c] = df[c].astype("string")

        df.to_csv(path, index=False, encoding="utf-8")
        print(f"✅ Saved {len(df)} rows to {path}")
        return path


def scrape_properties(
    urls_file: str = "/opt/airflow/data/urls/property_urls.csv",
    output_file: str = "properties_raw.csv",
    limit: Optional[int] = None,
) -> str:
    scraper = PropertyScraper()
    scraper.load_urls(urls_file, limit=limit)
    scraper.run(limit=limit)
    return scraper.save(output_file)


if __name__ == "__main__":
    scrape_properties(limit=50)

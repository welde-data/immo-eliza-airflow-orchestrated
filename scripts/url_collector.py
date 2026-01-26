"""
URL Collector Script - Adapted for Airflow
Scrapes property listing URLs from Immovlan.be and stores ONLY detail page URLs.

Key guarantees:
- Only keeps URLs containing "/en/detail/" (drops projectdetail pages)
- Converts relative URLs to absolute
- Deduplicates URLs while preserving order
"""

from __future__ import annotations

import csv
import os
import random
import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


BASE_URL = "https://immovlan.be"


class ListingScraper:
    """
    Collects listing URLs from Immovlan listing pages.
    """

    def __init__(self, output_dir: str = "/opt/airflow/data/urls") -> None:
        self.links: list[str] = []
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        # Optional: configurable wait (falls back to previous behavior if unset)
        self.page_wait_min = float(os.getenv("LISTING_PAGE_WAIT_MIN", "3"))
        self.page_wait_max = float(os.getenv("LISTING_PAGE_WAIT_MAX", "5"))

    def _sleep_page_load(self) -> None:
        time.sleep(random.uniform(self.page_wait_min, self.page_wait_max))

    def _accept_banners_once(self, driver) -> None:
        """
        Best-effort cookie/app banner handling. Safe to call multiple times.
        """
        # Cookie banner
        try:
            accept_button = driver.find_element(By.ID, "didomi-notice-agree-button")
            accept_button.click()
            print("Cookie banner accepted")
        except Exception:
            pass

        # App banner
        try:
            not_now_button = driver.find_element(By.CLASS_NAME, "button-link")
            not_now_button.click()
            print("Application banner closed")
        except Exception:
            pass

    def _normalize_and_filter_link(self, href: str) -> str | None:
        """
        Normalize to absolute URL and keep only detail pages.
        """
        if not href:
            return None

        # Convert relative -> absolute
        full = urljoin(BASE_URL, href)

        # Filter: only detail pages (drop projectdetail)
        if "/en/detail/" not in full:
            return None

        return full

    def get_listing_links(self, driver, url: str) -> None:
        """
        Loads a listing page and extracts detail URLs.
        """
        driver.get(url)
        self._sleep_page_load()

        # Try to accept banners if present
        self._accept_banners_once(driver)

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Extract links from listing cards
        for card in soup.find_all(class_="card-title ellipsis pr-2 mt-1 mb-0"):
            a = card.find("a")
            href = a.get("href") if a else None

            link = self._normalize_and_filter_link(href)
            if link:
                self.links.append(link)

    def call_driver(self, max_pages: int = 2) -> None:
        """
        Runs Selenium over provinces/pages to collect listing URLs.
        """
        provinces = [
            "antwerp",
            "vlaams-brabant",
            "brabant-wallon",
            "east-flanders",
            "west-flanders",
            "hainaut",
            "brussels",
            "liege",
            "limburg",
            "namur",
            "luxembourg",
        ]

        # Start virtual display for headless Chrome
        display = Display(visible=0, size=(1920, 1080))
        display.start()

        ua = UserAgent()

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("start-maximized")
        options.add_argument(f"user-agent={ua.random}")
        options.add_argument("--enable-javascript")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        driver = webdriver.Chrome(options=options)
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
        )

        try:
            for province in provinces:
                print(f"\n=== Scraping province: {province} ===")
                for page in range(1, max_pages + 1):
                    listing_url = (
                        f"{BASE_URL}/en/real-estate?"
                        f"transactiontypes=for-sale,in-public-sale&"
                        f"propertytypes=house,apartment&"
                        f"provinces={province}&"
                        f"islifeannuity=no&"
                        f"page={page}&"
                        f"noindex=1"
                    )

                    print(f"Scraping page {page} of {province}")
                    self.get_listing_links(driver, listing_url)

        finally:
            driver.quit()
            display.stop()

        # Deduplicate while preserving order
        self.links = list(dict.fromkeys(self.links))

        print(f"\n✅ Total detail URLs collected (deduped): {len(self.links)}")

    def save_to_csv(self, file_name: str = "property_urls.csv") -> str:
        file_path = os.path.join(self.output_dir, file_name)

        with open(file_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["link"])
            for link in self.links:
                writer.writerow([link])

        print(f"✅ Saved {len(self.links)} URLs to {file_path}")
        return file_path


def collect_urls(max_pages: int = 2, output_file: str = "property_urls.csv") -> str:
    scraper = ListingScraper()
    scraper.call_driver(max_pages=max_pages)
    return scraper.save_to_csv(output_file)


if __name__ == "__main__":
    collect_urls(max_pages=2)

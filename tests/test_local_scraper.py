import time
import random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def run_verified_test():
    print("🧪 [STEP 1] Initializing Test with JS-Click & BS4...")
    
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

    try:
        driver = webdriver.Remote(command_executor="http://localhost:4444/wd/hub", options=options)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return

    try:
        # Targeting Brussels Apartments for Sale
        target_url = "https://immovlan.be/en/real-estate?transactiontypes=for-sale,in-public-sale&propertytypes=house,apartment&provinces=brussels&islifeannuity=no&page=1&noindex=1"
        print(f"📡 [STEP 2] Navigating to: {target_url}")
        driver.get(target_url)
        
        # --- RULE: 7s DISCOVERY SLEEP ---
        print("💤 Discovery Sleep: 7 seconds...")
        time.sleep(7)

        # Forceful JS Cookie Click
        try:
            print("🍪 Attempting to clear cookies via JavaScript...")
            driver.execute_script("""
                const btn = document.querySelector('#didomi-notice-agree-button');
                if (btn) btn.click();
            """)
            time.sleep(2)
        except:
            print("ℹ️ Cookie button not interactable.")

        # Trigger Lazy Load
        driver.execute_script("window.scrollTo(0, 1000);")
        time.sleep(2)

        # --- BEAUTIFUL SOUP EXTRACTION ---
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # We search for your reference class AND a broader pattern
        links_found = []
        
        # Strategy 1: Your exact class
        cards = soup.find_all(class_="card-title ellipsis pr-2 mt-1 mb-0")
        
        # Strategy 2: Broad search if Strategy 1 fails
        if not cards:
            print("⚠️ Exact class match failed. Broadening search...")
            # Look for any <a> tags that contain the detail/sale pattern
            cards = soup.find_all("a", href=True)
            for a in cards:
                href = a['href']
                if "/en/detail/sale/" in href:
                    if href.startswith('/'):
                        href = "https://immovlan.be" + href
                    if href not in links_found:
                        links_found.append(href)
        else:
            for card in cards:
                a_tag = card.find("a")
                if a_tag and a_tag.get("href"):
                    link = a_tag.get("href")
                    if link.startswith('/'):
                        link = "https://immovlan.be" + link
                    links_found.append(link)

        print(f"\n--- DATA ANALYSIS ---")
        print(f"📊 Total unique 'For Sale' links: {len(links_found)}")

        if len(links_found) > 0:
            for i, l in enumerate(links_found[:5]):
                print(f"✅ [{i+1}] {l}")
            print("\n✨ TEST SUCCESSFUL")
        else:
            print("❌ Still 0. Let's dump the current URL and a bit of body text:")
            print(f"🔗 Current URL: {driver.current_url}")
            print(f"📄 Body snippet: {driver.find_element(By.TAG_NAME, 'body').text[:150]}...")

    finally:
        driver.quit()

if __name__ == "__main__":
    run_verified_test()
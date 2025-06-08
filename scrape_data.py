import os
import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

# === CONFIG ===
CHROME_DRIVER_PATH = "./chromedriver"
BASE_URL = "https://www.atlantafed.org/banking-and-payments/reporting/fry6"
OUTPUT_PATH = "pdf_urls.json"

# === HEADLESS BROWSER SETUP ===
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--window-size=1920,1080")

service = Service(CHROME_DRIVER_PATH)
driver = webdriver.Chrome(service=service, options=chrome_options)
driver.get(BASE_URL)

# === DISMISS COOKIE BANNER ===
try:
    cookie_banner = WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.CLASS_NAME, "global-cookie-banner"))
    )
    driver.execute_script("arguments[0].style.display = 'none';", cookie_banner)
    print("Hid cookie banner")
except:
    print("No cookie banner found")

pdf_urls = []

# === WAIT FOR TABLE ===
def wait_for_table():
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
    )

# === SCRAPE PDF LINKS ===
while True:
    wait_for_table()
    time.sleep(1)

    links = driver.find_elements(By.XPATH, "//table//a[contains(@href, '.pdf')]")
    for link in links:
        href = link.get_attribute("href")
        if href and href.endswith(".pdf") and href not in pdf_urls:
            pdf_urls.append(href)

    print(f" Collected {len(pdf_urls)} PDF URLs so far...")

    try:
        next_button = driver.find_element(By.CSS_SELECTOR, "a.k-pager-nav[title*='next']")
        if "k-state-disabled" in next_button.get_attribute("class"):
            print("Reached last page.")
            break
        else:
            next_button.click()
            time.sleep(2)
    except Exception as e:
        print(" No more pages or error with pagination:", e)
        break

driver.quit()

# === SAVE TO FILE ===
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(pdf_urls, f, indent=2)

print(f"\n Done! Total PDF links collected: {len(pdf_urls)}")
print(f" URLs saved to {OUTPUT_PATH}")

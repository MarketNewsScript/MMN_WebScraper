import os
import re
import time
import random
import smtplib
import requests
from typing import Optional, Tuple
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urljoin, urlparse, unquote
from bs4 import BeautifulSoup

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.storage.blob import BlobServiceClient

# === CONFIG ===
LIST_URL = "https://mymarketnews.ams.usda.gov/filerepo/reports?field_slug_id_value=3661&page=0"
BASE = "https://mymarketnews.ams.usda.gov"
AZURE_CONNECTION_STRING = os.environ['AZURE_CONNECTION_STRING']
AZURE_CONTAINER_NAME = "ams"
AZURE_BLOB_DIRECTORY = "Market News/USDA Weekly Reports/"
LATEST_SEEN_BLOB = AZURE_BLOB_DIRECTORY + "latest_seen.txt"

# Timeouts: (connect, read)
CONNECT_TIMEOUT = 8
READ_TIMEOUT = 45
TIMEOUT: Tuple[int, int] = (CONNECT_TIMEOUT, READ_TIMEOUT)

# Backoff / retry policy (shorter so we fail fast)
RETRIES_TOTAL = 3
BACKOFF_FACTOR = 1.5
STATUS_FORCELIST = (429, 500, 502, 503, 504)

# Hard overall watchdog: kill the run if it exceeds this many seconds
GLOBAL_DEADLINE_SECONDS = 180

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-US,en;q=0.6",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://mymarketnews.ams.usda.gov/",
}

# === EMAIL ===
def send_notification_email():
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    sender_email = os.environ['GMAIL_USER']
    sender_password = os.environ['GMAIL_PASSWORD']
    recipient_email = os.environ['RECIPIENT']
    subject = "USDA Web Scraper Script Notification"
    body = "The USDA Web Scraper script has finished running."

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = recipient_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_email, msg.as_string())
        print("[EMAIL] Notification sent.")
    except Exception as e:
        print(f"[EMAIL ERROR] Failed to send email {e}")

# === WATCHDOG ===
START_TS = time.time()
def time_left() -> float:
    return GLOBAL_DEADLINE_SECONDS - (time.time() - START_TS)

def check_deadline(point: str=""):
    left = time_left()
    if left <= 0:
        raise TimeoutError(f"Global deadline exceeded while {point or 'running'} (>{GLOBAL_DEADLINE_SECONDS}s).")

# === HTTP SESSION WITH RETRIES ===
def build_session() -> requests.Session:
    retry = Retry(
        total=RETRIES_TOTAL,
        connect=RETRIES_TOTAL,
        read=RETRIES_TOTAL,
        status=RETRIES_TOTAL,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=STATUS_FORCELIST,
        allowed_methods={"GET", "HEAD"},
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s = requests.Session()
    s.headers.update(headers)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

session = build_session()

def get_with_jitter(url: str, *, stream: bool=False) -> requests.Response:
    check_deadline(f"fetching {url}")
    time.sleep(random.uniform(0.4, 1.2))
    resp = session.get(url, timeout=TIMEOUT, stream=stream, allow_redirects=True)
    resp.raise_for_status()
    return resp

# === AZURE ===
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

def read_latest_seen() -> Optional[str]:
    try:
        blob_client = container_client.get_blob_client(LATEST_SEEN_BLOB)
        data = blob_client.download_blob().readall().decode("utf-8").strip()
        return data if data else None
    except Exception:
        return None

def write_latest_seen(value: str) -> None:
    blob_client = container_client.get_blob_client(LATEST_SEEN_BLOB)
    blob_client.upload_blob(value, overwrite=True)

def normalize_filename_from_url(url: str) -> str:
    path = urlparse(url).path
    name = os.path.basename(path)
    return unquote(name)

# === CORE SCRAPE (requests) ===
def parse_list_html(html: str):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        raise RuntimeError("Could not find table on list page.")
    tbody = table.find("tbody") or table
    first_row = tbody.find("tr")
    if not first_row:
        raise RuntimeError("No rows found on list page.")
    return first_row

def extract_date_from_row(row) -> Optional[str]:
    tds = row.find_all("td")
    for td in tds:
        txt = td.get_text(strip=True)
        m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b|\b(\d{2}-\d{2}-\d{4})\b", txt)
        if m:
            ds = m.group(1) or m.group(2)
            if re.match(r"\d{4}-\d{2}-\d{2}", ds):
                y, mm, dd = ds.split("-")
                return f"{mm}-{dd}-{y}"
            return ds
    return None

def extract_detail_url_from_row(row) -> str:
    detail_a = None
    for a in row.find_all("a", href=True):
        if (a.get_text() or "").strip().lower() == "view report":
            detail_a = a
            break
    if not detail_a:
        links = row.find_all("a", href=True)
        if not links:
            raise RuntimeError("No links in latest row.")
        detail_a = links[-1]
    return urljoin(BASE, detail_a["href"])

def extract_pdf_from_detail_html(html: str) -> str:
    dsoup = BeautifulSoup(html, "html.parser")
    pdf_a = dsoup.find("a", href=lambda h: h and h.lower().endswith(".pdf"))
    if not pdf_a:
        raise RuntimeError("Could not find PDF link on detail page.")
    return urljoin(BASE, pdf_a["href"])

def scrape_with_requests():
    r = get_with_jitter(LIST_URL)
    row = parse_list_html(r.text)
    report_date_str = extract_date_from_row(row)
    detail_url = extract_detail_url_from_row(row)
    dr = get_with_jitter(detail_url)
    pdf_url = extract_pdf_from_detail_html(dr.text)
    filename = f"National Hemp Report {report_date_str}.pdf" if report_date_str else normalize_filename_from_url(pdf_url)
    return detail_url, pdf_url, filename

# === SELENIUM FALLBACK (headless Chrome w/ stealth) ===
def scrape_with_selenium():
    check_deadline("initializing Selenium")
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    opts = uc.ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1200,2000")

    driver = uc.Chrome(options=opts)
    try:
        driver.get(LIST_URL)
        WebDriverWait(driver, min(30, int(max(5, time_left())))).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
        )
        html = driver.page_source
        row = parse_list_html(html)
        report_date_str = extract_date_from_row(row)
        detail_url = extract_detail_url_from_row(row)

        driver.get(detail_url)
        WebDriverWait(driver, min(30, int(max(5, time_left())))).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href$='.pdf']"))
        )
        html2 = driver.page_source
        pdf_url = extract_pdf_from_detail_html(html2)
        filename = f"National Hemp Report {report_date_str}.pdf" if report_date_str else normalize_filename_from_url(pdf_url)
        return detail_url, pdf_url, filename
    finally:
        driver.quit()

def scrape_latest_detail_and_pdf():
    # Try requests twice quickly; if still failing, switch to Selenium
    for attempt in range(2):
        try:
            check_deadline("scrape_with_requests")
            return scrape_with_requests()
        except Exception as e:
            print(f"[WARN] requests scrape attempt {attempt+1} failed: {e}")
            time.sleep(1.0)

    print("[INFO] Falling back to Selenium (headless)")
    return scrape_with_selenium()

def main():
    print(f"[INFO] Global watchdog: {GLOBAL_DEADLINE_SECONDS}s")
    latest_seen = read_latest_seen()
    print(f"[INFO] latest_seen: {latest_seen!r}")

    detail_url, pdf_url, filename = scrape_latest_detail_and_pdf()
    print(f"[LATEST] detail={detail_url}")
    print(f"[LATEST] pdf={pdf_url}")
    print(f"[LATEST] target filename={filename}")

    if latest_seen and latest_seen == pdf_url:
        print("[INFO] No new report. Exiting.")
        send_notification_email()
        return

    # Upload to Azure
    blob_path = AZURE_BLOB_DIRECTORY + filename

    # Skip if blob already exists (idempotent)
    exists = False
    try:
        container_client.get_blob_client(blob_path).get_blob_properties()
        exists = True
    except Exception:
        pass

    if exists:
        print(f"[SKIP] Already exists in Azure: {blob_path}")
        write_latest_seen(pdf_url)
        send_notification_email()
        return

    # Download PDF (requests; if it fails once, try again, then fail fast)
    for attempt in range(2):
        try:
            check_deadline("downloading PDF")
            pr = get_with_jitter(pdf_url)
            container_client.upload_blob(name=blob_path, data=pr.content, overwrite=True)
            print(f"[SUCCESS] Uploaded to Azure: {blob_path}")
            break
        except Exception as e:
            if attempt == 0:
                print(f"[WARN] PDF download attempt 1 failed: {e}; retrying...")
                time.sleep(1.0)
            else:
                raise RuntimeError(f"Failed to download/upload PDF after retries: {e}")

    # Update latest marker
    write_latest_seen(pdf_url)
    print("[INFO] latest_seen.txt updated.")
    send_notification_email()

if __name__ == "__main__":
    main()

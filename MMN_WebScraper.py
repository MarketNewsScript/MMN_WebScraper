import os
import re
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urljoin, urlparse, unquote
from bs4 import BeautifulSoup

from azure.storage.blob import BlobServiceClient

# === CONFIG ===
LIST_URL = "https://mymarketnews.ams.usda.gov/filerepo/reports?field_slug_id_value=3661&page=0"
BASE = "https://mymarketnews.ams.usda.gov"
AZURE_CONNECTION_STRING = os.environ['AZURE_CONNECTION_STRING']
AZURE_CONTAINER_NAME = "ams"
AZURE_BLOB_DIRECTORY = "Market News/USDA Weekly Reports/"
LATEST_SEEN_BLOB = AZURE_BLOB_DIRECTORY + "latest_seen.txt"

headers = {
    # IMPORTANT: no escaping/backslashes in header values
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
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


# === AZURE ===
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

def read_latest_seen() -> str | None:
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

def scrape_latest_detail_and_pdf():
    # 1) Fetch the list page
    r = requests.get(LIST_URL, headers=headers, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # 2) Find the first (latest) row
    table = soup.find("table")
    if not table:
        raise RuntimeError("Could not find table on list page.")
    tbody = table.find("tbody") or table
    first_row = tbody.find("tr")
    if not first_row:
        raise RuntimeError("No rows found on list page.")

    # Try to extract Report Date from the row (format YYYY-MM-DD or similar)
    # Adjust selector if the date is in a specific <td> position
    tds = first_row.find_all("td")
    report_date_str = None
    for td in tds:
        # Match 2025-08-27 or 08-27-2025
        m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b|\b(\d{2}-\d{2}-\d{4})\b", td.get_text(strip=True))
        if m:
            report_date_str = m.group(1) or m.group(2)
            break

    # Convert YYYY-MM-DD -> MM-DD-YYYY for filename if needed
    if report_date_str and re.match(r"\d{4}-\d{2}-\d{2}", report_date_str):
        y, m, d = report_date_str.split("-")
        report_date_str = f"{m}-{d}-{y}"

    # 3) Find the "view report" link in that row
    detail_a = None
    # Prefer link whose text contains 'view report'
    for a in first_row.find_all("a", href=True):
        if (a.get_text() or "").strip().lower() == "view report":
            detail_a = a
            break
    if not detail_a:
        # Fallback: last link in row
        links = first_row.find_all("a", href=True)
        if not links:
            raise RuntimeError("No links in latest row.")
        detail_a = links[-1]

    detail_url = urljoin(BASE, detail_a["href"])

    # 4) Fetch detail page and locate the single PDF link
    dr = requests.get(detail_url, headers=headers, timeout=30)
    dr.raise_for_status()
    dsoup = BeautifulSoup(dr.text, "html.parser")
    pdf_a = dsoup.find("a", href=lambda h: h and h.lower().endswith(".pdf"))
    if not pdf_a:
        raise RuntimeError("Could not find PDF link on detail page.")
    pdf_url = urljoin(BASE, pdf_a["href"])

    # 5) Determine target filename (prefer pretty name)
    if report_date_str:
        filename = f"National Hemp Report {report_date_str}.pdf"
    else:
        # fall back to the server provided filename
        filename = normalize_filename_from_url(pdf_url)

    return detail_url, pdf_url, filename

def main():
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
        write_latest_seen(pdf_url)  # still update marker so we don't recheck
        send_notification_email()
        return

    # Download & upload
    pr = requests.get(pdf_url, headers=headers, timeout=60)
    pr.raise_for_status()
    container_client.upload_blob(name=blob_path, data=pr.content, overwrite=True)
    print(f"[SUCCESS] Uploaded to Azure: {blob_path}")

    # Update latest marker
    write_latest_seen(pdf_url)
    print("[INFO] latest_seen.txt updated.")
    send_notification_email()

if __name__ == "__main__":
    main()

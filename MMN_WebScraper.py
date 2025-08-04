import requests
from azure.storage.blob import BlobServiceClient
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

BASE_URL = "https://mymarketnews.ams.usda.gov/filerepo/reports?field_slug_id_value=3661&page={}"
NUM_PAGES = 5
AZURE_CONNECTION_STRING = os.environ['AZURE_CONNECTION_STRING']
AZURE_CONTAINER_NAME = "ams"
AZURE_BLOB_DIRECTORY = "Market News/USDA Weekly Reports/"
LAST_UPLOADED_BLOB = AZURE_BLOB_DIRECTORY + "last_uploaded.txt"

headers = {
    "User-Agent": "...",  # Fill in as before
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://mymarketnews.ams.usda.gov/",
    # Add cookies from your browser if needed
}

# Email's script owner everytime script is ran.
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

# === SETUP AZURE CLIENT ===
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

# Read last_uploaded.txt from Azure
uploaded_files = set()
try:
    blob_client = container_client.get_blob_client(LAST_UPLOADED_BLOB)
    data = blob_client.download_blob().readall().decode('utf-8')
    uploaded_files = set(line.strip() for line in data.splitlines() if line.strip())
    print(f"[INFO] {len(uploaded_files)} files found in last_uploaded.txt")
except Exception as e:
    print(f"[INFO] last_uploaded.txt not found, assuming no files uploaded yet.")

newly_uploaded = []

for page in range(NUM_PAGES):
    page_url = BASE_URL.format(page)
    print(f"[PAGE {page+1}] Opening: {page_url}")
    try:
        resp = requests.get(page_url, headers=headers, timeout=30)
        print("[INFO] Page downloaded")
    except Exception as e:
        print(f"[ERROR] Failed to download page: {e}")
        continue
    html = resp.text

    # --- Find all PDF links in the HTML ---
    links = []
    idx = 0
    while True:
        idx = html.find('.pdf', idx)
        if idx == -1:
            break
        start_quote = html.rfind('"', 0, idx)
        if html[start_quote-5:start_quote] == 'href=':
            link = html[start_quote+1:idx+4]
            links.append(link)
        idx += 4

    print(f"Found {len(links)} PDF links on page {page+1}")

    for href in links:
        if not href.startswith('http'):
            href = "https://mymarketnews.ams.usda.gov" + href
        filename = os.path.basename(href)

        if filename in uploaded_files:
            print(f"[SKIP] Already in last_uploaded.txt: {filename}")
            continue

        blob_path = AZURE_BLOB_DIRECTORY + filename

        # Check if file already exists in Azure (optional, can remove if relying only on last_uploaded.txt)
        exists = False
        try:
            container_client.get_blob_client(blob_path).get_blob_properties()
            exists = True
        except Exception:
            pass

        if exists:
            print(f"[SKIP] Already exists in Azure: {blob_path}")
            continue

        print(f"[DOWNLOAD & UPLOAD] {filename}")
        try:
            pdf_response = requests.get(href, headers=headers, timeout=60)
            pdf_response.raise_for_status()
            container_client.upload_blob(name=blob_path, data=pdf_response.content, overwrite=True)
            print(f"[SUCCESS] Uploaded to Azure: {blob_path}")
            newly_uploaded.append(filename)
        except Exception as e:
            print(f"[ERROR] Failed to download or upload {filename}: {e}")

# Update last_uploaded.txt in Azure
if newly_uploaded:
    try:
        all_uploaded = uploaded_files.union(newly_uploaded)
        blob_client = container_client.get_blob_client(LAST_UPLOADED_BLOB)
        blob_client.upload_blob("\n".join(all_uploaded), overwrite=True)
        print("[INFO] last_uploaded.txt updated")
    except Exception as e:
        print(f"[ERROR] Failed to update last_uploaded.txt: {e}")
else:
    print("[INFO] No new files uploaded. last_uploaded.txt not updated.")

print("[COMPLETE] All pages processed.")

# === SEND EMAIL NOTIFICATION ===
send_notification_email()

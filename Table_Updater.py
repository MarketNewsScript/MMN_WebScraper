from azure.storage.blob import BlobServiceClient
import os
import re
from datetime import datetime

# === CONFIGURATION ===
azure_connection_string = os.environ['AZURE_CONNECTION_STRING']
container_name = 'ams'
account_name = 'busercapstone'
folder_prefix = 'Market News/USDA Weekly Reports/'
output_blob_path = 'Market News/MMN_LIST.html'

# === INITIALIZE BLOB SERVICE ===
blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_client = blob_service_client.get_container_client(container_name)

# === GET LIST OF BLOBS IN FOLDER ===
blob_list = list(container_client.list_blobs(name_starts_with=folder_prefix))

# === EXTRACT DATE FROM FILENAME FUNCTION ===
date_regex = re.compile(r'(\d{2})-(\d{2})-(\d{4})')  # For MM-DD-YYYY

def extract_date(blob_name):
    filename = os.path.basename(blob_name)
    match = date_regex.search(filename)
    if match:
        # MM-DD-YYYY to datetime
        return datetime.strptime(f"{match.group(1)}-{match.group(2)}-{match.group(3)}", "%m-%d-%Y")
    else:
        return datetime.min  # Push files without dates to the bottom

# === BUILD ROWS WITH SORTING ===
rows = []
for blob in blob_list:
    filename = os.path.basename(blob.name)
    if filename == "last_uploaded.txt":
        continue  # skip tracker file
    if not blob.name.startswith(folder_prefix):
        continue
    file_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob.name.replace(' ', '%20')}"
    file_date = extract_date(blob.name)
    row = {
        "html": f"<tr><td>{filename}</td><td><a href='{file_url}' target='_blank'>Download</a></td></tr>",
        "date": file_date
    }
    rows.append(row)

# Sort rows DESCENDING (most recent first)
rows.sort(key=lambda x: x["date"], reverse=True)

# JS array of HTML rows
js_rows = ',\n'.join([f'"{r["html"].replace(chr(34), "&quot;")}"' for r in rows])

html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>USDA Weekly Reports</title>
    <style>
        body {{ font-family: Arial, sans-serif; background: #f8f9fa; }}
        h1 {{ text-align: center; }}
        #pagination {{ text-align: center; margin: 20px auto 10px auto; }}
        table {{ border-collapse: collapse; width: 80%; margin: 0 auto; background: #fff; }}
        th, td {{ border: 1px solid #ccc; padding: 12px 16px; text-align: left; }}
        th {{ background-color: #294d36; color: #fff; }}
        tr:nth-child(even) {{ background: #f2f2f2; }}
        .page-btn {{
            margin: 0 2px;
            padding: 4px 10px;
            background: #294d36;
            color: #fff;
            border: none;
            border-radius: 5px;
            cursor: pointer;
        }}
        .page-btn.active, .page-btn:focus {{
            background: #31775c;
            outline: none;
        }}
    </style>
</head>
<body>
    <h1>USDA Weekly Reports</h1>
    <div id="pagination"></div>
    <table>
        <thead>
            <tr><th>File Name</th><th>Download Link</th></tr>
        </thead>
        <tbody id="table-body">
            <!-- Data goes here -->
        </tbody>
    </table>
    <script>
        const allRows = [
            {js_rows}
        ];
        const rowsPerPage = 20;
        let currentPage = 1;
        const totalPages = Math.ceil(allRows.length / rowsPerPage);

        function showPage(page) {{
            currentPage = page;
            const start = (page - 1) * rowsPerPage;
            const end = start + rowsPerPage;
            document.getElementById('table-body').innerHTML = allRows.slice(start, end).join('');
            renderPagination();
        }}

        function renderPagination() {{
            let html = '';
            for(let i=1; i<=totalPages; i++) {{
                html += `<button class="page-btn${{i===currentPage?' active':''}}" onclick="showPage(${{i}})">${{i}}</button>`;
            }}
            document.getElementById('pagination').innerHTML = html;
        }}

        // Initial load
        showPage(1);
    </script>
</body>
</html>
"""

# === UPLOAD HTML TO BLOB (OVERWRITE IF EXISTS) ===
blob_client = container_client.get_blob_client(output_blob_path)
blob_client.upload_blob(html_content, overwrite=True)

print(f"HTML file generated and uploaded to {output_blob_path}")

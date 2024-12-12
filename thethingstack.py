import requests
import json
import time
from datetime import datetime
import os
import csv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

DEFAULT_BASE_PATH = "/app/data"  # Directorio base en Docker


application_id = "lab-quercus"
device_id = "dragino-ltc2-device"
api_key = "Bearer NNSXS.2NANY2ZQ4GUYLG3FK6NIQYFWB7HSOTO7VZIIJWY.LU2DGJU2NBCF5RHVZSZS6PF47LY7YRLBAYYVGWK34FGJY6IOEAXQ"
url = f"https://eu1.cloud.thethings.network/api/v3/as/applications/{application_id}/devices/{device_id}/packages/storage/uplink_message"

headers = {
    "Authorization": api_key,
    "Accept": "text/event-stream"
}

os.makedirs(DEFAULT_BASE_PATH, exist_ok=True)


def process_data(response):
    filtered_data = []
    for line in response.text.splitlines():
        if line:
            try:
                data = json.loads(line)
                received_at = data.get("result", {}).get("received_at")
                name = data.get("result", {}).get("end_device_ids", {}).get("device_id")
                decoded_payload = data.get("result", {}).get("uplink_message", {}).get("decoded_payload", {})

                if received_at and decoded_payload:
                    filtered_data.append({
                        "received_at": received_at,
                        "name": name,
                        "decoded_payload": decoded_payload
                    })
            except json.JSONDecodeError as e:
                print(f"Error decoding a JSON line: {e}")
    return filtered_data

def save_data(data, filename):
    filename = os.path.join(DEFAULT_BASE_PATH, f"TTN_{filename}")  # Prefijo TTN_ y ruta completa
    with open(filename, "w") as json_file:
        json.dump(data, json_file, indent=4)
    print(f"Datos guardados en '{filename}'")

def fetch_data(params, filename):
    all_data = []
    while True:
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            if response.headers.get("Content-Type") == "text/event-stream":
                data = process_data(response)
                all_data.extend(data)
                if len(data) < 1000:
                    break  # Exit loop if no more data
                params["after"] = data[-1]["received_at"]
            else:
                print("Error: The response is not text/event-stream.")
                print("Response content:", response.text)
                break
        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 429:
                print("Error 429: Too many requests. Retrying in 10 seconds...")
                time.sleep(10)
            elif response.status_code == 400:
                print("Error 400: Check the URL, identifiers, or AppKey format.")
                break
            else:
                print(f"Request error: {http_err}")
                break
        except Exception as err:
            print(f"Unexpected error: {err}")
            break
    save_data(all_data, filename)

def json_to_csv(json_filename):
    csv_filename = json_filename.replace(".json", ".csv")
    json_filename = os.path.join(DEFAULT_BASE_PATH, json_filename)  # Ruta completa
    csv_filename = os.path.join(DEFAULT_BASE_PATH, csv_filename)  # Ruta completa para CSV
    
    with open(json_filename) as json_file:
        data = json.load(json_file)

    headers = set(["received_at", "name"])
    for entry in data:
        headers.update(entry.get("decoded_payload", {}).keys())

    with open(csv_filename, mode="w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(headers))
        writer.writeheader()
        
        for entry in data:
            row = {"received_at": entry.get("received_at"), "name": entry.get("name")}
            row.update(entry.get("decoded_payload", {}))
            writer.writerow(row)
    
    print(f"CSV creado: '{csv_filename}'")
    return csv_filename

def send_email(filename, subject, body):
    sender = "tfgawssensorcloud@gmail.com"
    recipient = "tfgawssensorcloud@gmail.com"

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = recipient
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    with open(filename, "rb") as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename= {os.path.basename(filename)}")
        msg.attach(part)

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender, 'wpfv tezm llhi fabw')
        server.send_message(msg)
        server.quit()
        print(f"CSV enviado con el nombre de {filename}.")
    except Exception as e:
        print(f"Error sending email: {e}")

def download_full_history():
    params = {"limit": 1000}
    filename = "full_history.json"
    fetch_data(params, filename)
    csv_filename = json_to_csv(f"TTN_{filename}")
    send_email(csv_filename, "Full Data History Report", "Attached is the CSV report for the full data history.")

def download_between_dates(start_date, end_date):
    params = {
        "after": start_date + "T00:00:00Z",
        "before": end_date + "T23:59:59Z",
        "limit": 1000
    }
    filename = f"history_{start_date}_to_{end_date}.json"
    fetch_data(params, filename)
    csv_filename = json_to_csv(f"TTN_{filename}")
    send_email(csv_filename, "Data Report Between Dates", f"Attached is the CSV report for data between {start_date} and {end_date}.")

def download_from_date(start_date):
    params = {
        "after": start_date + "T00:00:00Z",
        "limit": 1000
    }
    filename = f"history_from_{start_date}.json"
    fetch_data(params, filename)
    csv_filename = json_to_csv(f"TTN_{filename}")
    send_email(csv_filename, "Data Report From Date", f"Attached is the CSV report for data from {start_date} onwards.")

def download_to_date(end_date):
    params = {
        "before": end_date + "T23:59:59Z",
        "limit": 1000
    }
    filename = f"history_to_{end_date}.json"
    fetch_data(params, filename)
    csv_filename = json_to_csv(f"TTN_{filename}")
    send_email(csv_filename, "Data Report To Date", f"Attached is the CSV report for data up to {end_date}.")

from datetime import datetime, timedelta
import json
import requests
import csv
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

DEFAULT_BASE_PATH = "/app/data"


def build_url(channel_id):
    return f"https://api.thingspeak.com/channels/{channel_id}/feeds.json"

def add_common_params(params, api_key=None, results=8000, start=None):
    if api_key:
        params['api_key'] = api_key
    params['results'] = results
    if start:
        params['start'] = start
    return params

def fetch_data(url, params, filename,base_path=DEFAULT_BASE_PATH):
    all_data = []
    channel_info = None
    lote = 1
    max_lotes = 100
    last_entry_time = None

    os.makedirs(base_path, exist_ok=True)
    filename = os.path.join(base_path, filename)


    while lote <= max_lotes:
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if not channel_info:
                    channel_info = data['channel']
                feeds = data['feeds']
                if not feeds:
                    break

                all_data.extend(feeds)

                if len(feeds) < params['results']:
                    break

                last_entry_time = feeds[-1]['created_at']
                last_entry_datetime = datetime.strptime(last_entry_time, '%Y-%m-%dT%H:%M:%SZ') + timedelta(seconds=1)
                params['start'] = last_entry_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
                lote += 1

            else:
                print(f"Error {response.status_code}: {response.text}")
                break

        except requests.exceptions.Timeout:
            continue

    result = {
        "channel": channel_info,
        "feeds": all_data
    }

    with open(filename, 'w') as file:
        json.dump(result, file, indent=4)
    print(f"Datos guardados en '{filename}'")

    # Convertir JSON a CSV y enviar el archivo CSV por correo
    csv_filename = convert_json_to_csv(filename)
    send_email_with_attachment(csv_filename, "Reporte de Datos ThingSpeak", "Adjunto el archivo CSV con el reporte solicitado.")

def fetch_all_data(channel_id, api_key=None):
    url = build_url(channel_id)
    params = add_common_params({}, api_key)
    filename = 'TSPK_datosRecuperados_all.json'
    fetch_data(url, params, filename)

def get_data_by_fields(channel_id, fields, api_key=None, results=8000):
    for field in fields:
        url = f"https://api.thingspeak.com/channels/{channel_id}/fields/{field}.json"
        params = add_common_params({}, api_key, results)
        filename = f'TSPK_datosRecuperados_field_{field}.json'
        fetch_data(url, params, filename)

def get_data_from_date(channel_id, start_date=None, end_date=None, api_key=None, results=8000):
    url = build_url(channel_id)
    params = add_common_params({}, api_key, results)
    params['start'] = start_date
    params['end'] = end_date
    filename = f'TSPK_datosRecuperados_date_{start_date}_to_{end_date}.json'
    fetch_data(url, params, filename)

def get_data_from_date_forward(channel_id, start_date=None, api_key=None, results=8000):
    url = build_url(channel_id)
    params = add_common_params({}, api_key, results)
    if start_date:
        params['start'] = start_date
    filename = f'TSPK_datosRecuperados_forward_{start_date}.json'
    fetch_data(url, params, filename)

def get_data_from_date_backward(channel_id, start_date=None, api_key=None, results=8000):
    url = build_url(channel_id)
    params = add_common_params({}, api_key, results)
    if start_date:
        params['end'] = start_date
    filename = f'TSPK_datosRecuperados_backward_{start_date}.json'
    fetch_data(url, params, filename)

def convert_json_to_csv(json_filename):
    """Convierte el archivo JSON descargado en un archivo CSV con encabezados de campo personalizados."""
    csv_filename = json_filename.replace(".json", ".csv")

    with open(json_filename, 'r') as json_file:
        data = json.load(json_file)

    if not data.get("feeds"):
        print("No hay datos en el JSON para convertir a CSV.")
        return csv_filename

    # Obtener nombres personalizados de los campos desde la sección "channel"
    channel_info = data.get("channel", {})
    field_names = []
    for i in range(1, 100):  # Buscar hasta encontrar el primer campo vacío
        field_name = channel_info.get(f"field{i}")
        if not field_name:
            break
        field_names.append(field_name)

    # Crear archivo CSV con los encabezados personalizados
    with open(csv_filename, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        headers = ["created_at", "entry_id"] + field_names
        writer.writerow(headers)

        for entry in data["feeds"]:
            row = [entry.get("created_at"), entry.get("entry_id")] + [entry.get(f"field{i}", "") for i in range(1, len(field_names) + 1)]
            writer.writerow(row)

    return csv_filename

def send_email_with_attachment(filename, subject, body):
    """Envía un correo electrónico con el archivo adjunto especificado."""
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
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender, 'wpfv tezm llhi fabw')
            server.send_message(msg)
        print(f"Correo enviado con el archivo adjunto {filename}.")
    except Exception as e:
        print(f"Error enviando correo: {e}")

import pandas as pd
import mysql.connector
from sklearn.preprocessing import MinMaxScaler
from fpdf import FPDF
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os

# Ruta base para almacenar los archivos
DEFAULT_BASE_PATH = "/app/data"

conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),  # Endpoint de tu base de datos RDS
        user="root",       # Usuario de la base de datos
        password="password123",    # Contraseña de la base de datos
        database="mydatabase"  # Especificar la base de datos
)

query = "SELECT * FROM Tfg"
cursor = conn.cursor()
cursor.execute(query)

column_names = [desc[0] for desc in cursor.description]
data = pd.DataFrame(cursor.fetchall(), columns=column_names)
cursor.close()
conn.close()

if 'entry_id' in data.columns:
    data.drop(columns=['entry_id'], inplace=True)

if 'created_at' in data.columns:
    data['created_at'] = pd.to_datetime(data['created_at'])
data.dropna(inplace=True)  

scaler = MinMaxScaler()
numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns  # Solo columnas numéricas
data[numeric_columns] = scaler.fit_transform(data[numeric_columns])

reporte = ""

reporte += "=== Análisis de Anomalías ===\n"
anomalies = {}
for col in numeric_columns:
    mean = data[col].mean()
    std_dev = data[col].std()
    anomalies[col] = data[(data[col] < mean - 3 * std_dev) | (data[col] > mean + 3 * std_dev)]
    if anomalies[col].empty:
        reporte += f"No se detectaron anomalías en {col}.\n"
    else:
        reporte += f"Anomalías detectadas en {col}:\n{anomalies[col]}\n\n"

if 'created_at' in data.columns:
    data['hour'] = data['created_at'].dt.hour
    data['month'] = data['created_at'].dt.month

reporte += "\n=== Análisis Temporal ===\n"
for col in numeric_columns:
    if 'hour' in data.columns:
        hourly_mean = data.groupby('hour')[col].mean()
        reporte += f"Media de {col} por hora del día:\n{hourly_mean}\n\n"
    if 'month' in data.columns:
        monthly_mean = data.groupby('month')[col].mean()
        reporte += f"Media de {col} por mes:\n{monthly_mean}\n\n"

reporte += "\n=== Matriz de Correlación ===\n"
correlation_matrix = data[numeric_columns].corr()
reporte += f"Matriz de correlación:\n{correlation_matrix}\n\n"

reporte += "\n=== Situaciones Críticas Detectadas ===\n"
if all(col in data.columns for col in ['calidad_aire', 'temperatura', 'viento']):
    critical_conditions = data[
        (data['calidad_aire'] > 0.8) & (data['temperatura'] > 0.7) & (data['viento'] < 0.3)
    ]
    if critical_conditions.empty:
        reporte += "No se detectaron situaciones críticas bajo las condiciones definidas.\n"
    else:
        reporte += f"Situaciones críticas detectadas:\n{critical_conditions}\n\n"

reporte += "\n=== Detección de Cambios Bruscos ===\n"
for col in numeric_columns:
    data[f'{col}_diff'] = data[col].diff().abs()
    threshold = 0.2  
    abrupt_changes = data[data[f'{col}_diff'] > threshold]
    if abrupt_changes.empty:
        reporte += f"No se detectaron cambios bruscos significativos en {col}.\n"
    else:
        reporte += f"Cambios bruscos en {col}:\n{abrupt_changes}\n\n"

# Configuración del PDF
pdf = FPDF()
pdf.add_page()
pdf.set_font("Arial", size=12)
pdf.cell(200, 10, txt="Reporte de los Datos", ln=True, align='C')
for line in reporte.split("\n"):
    pdf.cell(200, 10, txt=line, ln=True)

# Guardar el PDF en la ruta especificada
os.makedirs(DEFAULT_BASE_PATH, exist_ok=True)
pdf_filename = os.path.join(DEFAULT_BASE_PATH, "reporte_datos.pdf")
pdf.output(pdf_filename)

def enviar_correo():
    remitente = "tfgawssensorcloud@gmail.com"
    destinatario = "tfgawssensorcloud@gmail.com"
    asunto = "Reporte de Calidad de los Datos"
    cuerpo = "Adjunto se envía el reporte de calidad de los datos generado automáticamente."

    msg = MIMEMultipart()
    msg['From'] = remitente
    msg['To'] = destinatario
    msg['Subject'] = asunto
    msg.attach(MIMEText(cuerpo, 'plain'))

    with open(pdf_filename, "rb") as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename= reporte_datos.pdf")
        msg.attach(part)

    servidor = smtplib.SMTP('smtp.gmail.com', 587)
    servidor.starttls()
    servidor.login(remitente, 'wpfv tezm llhi fabw') 
    servidor.send_message(msg)
    servidor.quit()
    print("Correo enviado con el reporte en PDF.")

enviar_correo()

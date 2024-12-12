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

# Conexión a la base de datos
conn = mysql.connector.connect(
    host=os.getenv("DB_HOST"),  # Endpoint de tu base de datos RDS
    user="root",       # Usuario de la base de datos
    password="password123",    # Contraseña de la base de datos
    database="mydatabase"  # Especificar la base de datos
)

# Consulta para obtener solo Temp_Channel1 y Temp_Channel2
query = "SELECT Temp_Channel1, Temp_Channel2 FROM `dragino-ltc2-device`;"
cursor = conn.cursor()
cursor.execute(query)

column_names = [desc[0] for desc in cursor.description]
data = pd.DataFrame(cursor.fetchall(), columns=column_names)
cursor.close()
conn.close()

# Verifica que los datos no estén vacíos antes de proceder
if data.empty:
    print("No hay datos disponibles en la base de datos para generar el reporte.")
    exit()

# Normalización de los datos numéricos
numeric_columns = ['Temp_Channel1', 'Temp_Channel2']
scaler = MinMaxScaler()
data[numeric_columns] = scaler.fit_transform(data[numeric_columns])

# Generación del reporte
reporte = "=== Análisis de Anomalías ===\n"
anomalies = {}
for col in numeric_columns:
    mean = data[col].mean()
    std_dev = data[col].std()
    anomalies[col] = data[(data[col] < mean - 3 * std_dev) | (data[col] > mean + 3 * std_dev)]
    if anomalies[col].empty:
        reporte += f"No se detectaron anomalías en {col}.\n"
    else:
        reporte += f"Anomalías detectadas en {col}:\n{anomalies[col]}\n\n"

# Análisis Temporal (agregado manualmente para simulación)
data['index'] = range(len(data))
reporte += "\n=== Análisis Temporal ===\n"
for col in numeric_columns:
    rolling_mean = data[col].rolling(window=5, min_periods=1).mean()
    reporte += f"Media móvil de {col} (ventana de 5):\n{rolling_mean}\n\n"

# Matriz de correlación
reporte += "\n=== Matriz de Correlación ===\n"
correlation_matrix = data[numeric_columns].corr()
reporte += f"Matriz de correlación:\n{correlation_matrix}\n\n"

# Detección de cambios bruscos
reporte += "\n=== Detección de Cambios Bruscos ===\n"
for col in numeric_columns:
    data[f'{col}_diff'] = data[col].diff().abs()
    threshold = 0.2  
    abrupt_changes = data[data[f'{col}_diff'] > threshold]
    if abrupt_changes.empty:
        reporte += f"No se detectaron cambios bruscos significativos en {col}.\n"
    else:
        reporte += f"Cambios bruscos en {col}:\n{abrupt_changes}\n\n"

# Generación del PDF
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

# Envío del correo electrónico con el PDF adjunto
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

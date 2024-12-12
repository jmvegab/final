import mysql.connector
from mysql.connector import Error
import requests
import os
import re
import json
import sys

# Conexión a la base de datos MySQL
def conectar_mysql():
    try:
        conexion = mysql.connector.connect(
            host=os.getenv("DB_HOST"),  # Endpoint de tu base de datos RDS
            user="root",       # Usuario de la base de datos
            password="password123",    # Contraseña de la base de datos
            database="mydatabase"  # Especificar la base de datos
        )
        if conexion.is_connected():
            print("Conexión exitosa a la base de datos MySQL")
        return conexion
    except Error as e:
        print(f"Error al conectar a MySQL: {e}")
        return None

# Datos del bot de Telegram
BOT_TOKEN = "7939888700:AAExa4QWbr16rEipOkbWkYuK1iCdPcUJ_d4"
CHAT_ID = "1301939829"

# Umbrales para Temp_Channel1 y Temp_Channel2
UMBRAL_TEMP_CHANNEL1_MIN = 0
UMBRAL_TEMP_CHANNEL1_MAX = 40
UMBRAL_TEMP_CHANNEL2_MIN = 0
UMBRAL_TEMP_CHANNEL2_MAX = 40

# Contador de alertas
contador_alertas = 0
LIMITE_ALERTAS_CRITICO = 20

# Función para enviar alerta a Telegram
def enviar_alerta(entry_id, created_at, campo, valor, limite_min, limite_max):
    global contador_alertas
    mensaje = (f"⚠️ Alerta:\n"
               f"Entrada con ID: {entry_id}\n"
               f"Fecha: {created_at}\n"
               f"{campo} superó el rango permitido: {valor} (límite: {limite_min}-{limite_max})")
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {'chat_id': CHAT_ID, 'text': mensaje}
    requests.post(url, data=payload)
    
    contador_alertas += 1
    if contador_alertas >= LIMITE_ALERTAS_CRITICO:
        enviar_alerta_critica()
        sys.exit("Programa terminado: Se ha alcanzado el límite de alertas críticas.")

def enviar_alerta_critica():
    """Envía un mensaje de estado crítico al bot de Telegram."""
    mensaje = "⚠️ Estado Crítico: Se han registrado más de 20 alertas. Es necesario revisar el sistema completo."
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {'chat_id': CHAT_ID, 'text': mensaje}
    requests.post(url, data=payload)

# Función para realizar la consulta SQL según el tipo de archivo
def consulta_por_tipo(cursor, nombre_tabla, nombre_archivo):
    print(f"Procesando archivo: {nombre_archivo}")

    # Caso 1: Si es 'TTN_full_history.json'
    if nombre_archivo == 'TTN_full_history.json':
        cursor.execute(f"SELECT * FROM `{nombre_tabla}`")
        return cursor.fetchall()

    # Caso 2: Si es entre dos fechas
    match_dates = re.match(r"TTN_history_(.+)_to_(.+)\.json", nombre_archivo)
    if match_dates:
        start_date = match_dates.group(1)
        end_date = match_dates.group(2)
        cursor.execute(f"SELECT * FROM `{nombre_tabla}` WHERE created_at BETWEEN '{start_date}' AND '{end_date}'")
        return cursor.fetchall()

    # Caso 3: Si es desde una fecha
    match_forward = re.match(r"TTN_history_from_(.+)\.json", nombre_archivo)
    if match_forward:
        start_date = match_forward.group(1)
        cursor.execute(f"SELECT * FROM `{nombre_tabla}` WHERE created_at >= '{start_date}'")
        return cursor.fetchall()

    # Caso 4: Si es hasta una fecha
    match_backward = re.match(r"TTN_history_to_(.+)\.json", nombre_archivo)
    if match_backward:
        end_date = match_backward.group(1)
        cursor.execute(f"SELECT * FROM `{nombre_tabla}` WHERE created_at <= '{end_date}'")
        return cursor.fetchall()

    return []

# Función para comprobar los datos de la base de datos y generar alertas
def comprobar_datos_y_enviar_alertas(nombre_archivo):
    conexion = conectar_mysql()
    cursor = conexion.cursor(dictionary=True)

    # Abrir el archivo JSON para obtener el nombre de la tabla
    with open(nombre_archivo, 'r') as archivo:
        datos_json = json.load(archivo)
        nombre_tabla = datos_json[0]['name']


    # Consultar según el tipo de archivo
    registros = consulta_por_tipo(cursor, nombre_tabla, nombre_archivo)

    # Revisar cada registro y comprobar si se superan los umbrales
    for registro in registros:
        entry_id = registro['entry_id']
        created_at = registro['created_at']
        temp_channel1 = registro.get('Temp_Channel1')
        temp_channel2 = registro.get('Temp_Channel2')

        # Comprobar si los valores superan los umbrales
        if temp_channel1 is not None and (temp_channel1 < UMBRAL_TEMP_CHANNEL1_MIN or temp_channel1 > UMBRAL_TEMP_CHANNEL1_MAX):
            enviar_alerta(entry_id, created_at, "Temp_Channel1", temp_channel1, UMBRAL_TEMP_CHANNEL1_MIN, UMBRAL_TEMP_CHANNEL1_MAX)
        if temp_channel2 is not None and (temp_channel2 < UMBRAL_TEMP_CHANNEL2_MIN or temp_channel2 > UMBRAL_TEMP_CHANNEL2_MAX):
            enviar_alerta(entry_id, created_at, "Temp_Channel2", temp_channel2, UMBRAL_TEMP_CHANNEL2_MIN, UMBRAL_TEMP_CHANNEL2_MAX)

    # Cerrar la conexión
    cursor.close()
    conexion.close()

# Ejecutar la comprobación y envío de alertas
if __name__ == "__main__":
    if len(sys.argv) > 1:
        nombre_archivo_json = sys.argv[1]
    else:
        nombre_archivo_json = 'TTN_full_history.json'
    comprobar_datos_y_enviar_alertas(nombre_archivo_json)

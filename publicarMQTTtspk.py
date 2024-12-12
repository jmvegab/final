import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import Error
import os
import statistics

# Configuración de las credenciales MQTT
MQTT_USERNAME = "HA87PA8pMRUeIzksBBU5KTI"
MQTT_PASSWORD = "wBdlHkSB5C3T21DvvKuHw3W9"
MQTT_CLIENT_ID = "HA87PA8pMRUeIzksBBU5KTI"
MQTT_BROKER = "mqtt3.thingspeak.com"
MQTT_PORT = 1883
CHANNEL_ID = 2694778  # ID del canal de estadísticas en ThingSpeak
MQTT_TOPIC = f"channels/{CHANNEL_ID}/publish"

# Función para calcular estadísticas avanzadas de los atributos en la base de datos
def calcular_estadisticas():
    try:
        conexion = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user="root",
            password="password123",
            database="mydatabase"
        )
        if conexion.is_connected():
            print("Conexión exitosa a la base de datos MySQL")

    except mysql.connector.Error as e:
        print(f"Error al conectar a MySQL: {e}")
        return None

    cursor = conexion.cursor(dictionary=True)

    # Realizar consulta para obtener los valores de cada atributo
    query = "SELECT temperatura, humedad, presion, calidad_aire, viento FROM Tfg;"
    cursor.execute(query)
    registros = cursor.fetchall()
    
    cursor.close()
    conexion.close()

    # Si hay datos, calcular estadísticas avanzadas
    if registros:
        temperatura = [r['temperatura'] for r in registros if r['temperatura'] is not None]
        humedad = [r['humedad'] for r in registros if r['humedad'] is not None]
        presion = [r['presion'] for r in registros if r['presion'] is not None]
        calidad_aire = [r['calidad_aire'] for r in registros if r['calidad_aire'] is not None]
        viento = [r['viento'] for r in registros if r['viento'] is not None]

        # Calcular estadísticas para cada atributo
        estadisticas = {
            "temperatura_promedio": round(statistics.mean(temperatura), 2) if temperatura else None,
            "temperatura_mediana": round(statistics.median(temperatura), 2) if temperatura else None,
            "humedad_desviacion": round(statistics.stdev(humedad), 2) if len(humedad) > 1 else None,
            "presion_maxima": max(presion) if presion else None,
            "presion_minima": min(presion) if presion else None,
            "calidad_aire_maxima": max(calidad_aire) if calidad_aire else None,
            "viento_promedio": round(statistics.mean(viento), 2) if viento else None,
            "viento_maxima": max(viento) if viento else None
        }
        
        return estadisticas

# Publicar las estadísticas en ThingSpeak usando MQTT
def publicar_estadisticas():
    estadisticas = calcular_estadisticas()
    if estadisticas:
        # Crear el payload con las estadísticas
        payload = {
            "field1": estadisticas['temperatura_promedio'],
            "field2": estadisticas['temperatura_mediana'],
            "field3": estadisticas['humedad_desviacion'],
            "field4": estadisticas['presion_maxima'],
            "field5": estadisticas['presion_minima'],
            "field6": estadisticas['calidad_aire_maxima'],
            "field7": estadisticas['viento_promedio'],
            "field8": estadisticas['viento_maxima']
        }
        
        # Publicar los datos en el canal de ThingSpeak
        client = mqtt.Client(client_id=MQTT_CLIENT_ID)
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        client.connect(MQTT_BROKER, MQTT_PORT, 60)

        # Convertir el payload a formato URL para enviarlo
        payload_str = '&'.join([f"{key}={value}" for key, value in payload.items() if value is not None])
        
        # Publicar el mensaje
        client.publish(MQTT_TOPIC, payload_str)
        print(f"Estadísticas publicadas: {payload_str}")

        # Desconectar del broker MQTT
        client.disconnect()

# Ejecutar la publicación de estadísticas
if __name__ == "__main__":
    publicar_estadisticas()

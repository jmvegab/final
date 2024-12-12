import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import Error
import statistics
import os
import json
import base64

MQTT_BROKER = "eu1.cloud.thethings.network"
MQTT_PORT = 1883
TTN_APP_ID = "lab-quercus"  
TTN_DEVICE_ID = "dragino-ltc2-device"  
TTN_ACCESS_KEY = "NNSXS.2NANY2ZQ4GUYLG3FK6NIQYFWB7HSOTO7VZIIJWY.LU2DGJU2NBCF5RHVZSZS6PF47LY7YRLBAYYVGWK34FGJY6IOEAXQ"  


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

    query = "SELECT Temp_Channel1, Temp_Channel2 FROM `dragino-ltc2-device`;"
    cursor.execute(query)
    registros = cursor.fetchall()
    
    cursor.close()
    conexion.close()

    if registros:
        temp_channel1 = [r['Temp_Channel1'] for r in registros if r['Temp_Channel1'] is not None]
        temp_channel2 = [r['Temp_Channel2'] for r in registros if r['Temp_Channel2'] is not None]

        estadisticas = {
            "temp_channel1_promedio": round(statistics.mean(temp_channel1), 2) if temp_channel1 else None,
            "temp_channel1_mediana": round(statistics.median(temp_channel1), 2) if temp_channel1 else None,
            "temp_channel2_promedio": round(statistics.mean(temp_channel2), 2) if temp_channel2 else None,
            "temp_channel2_mediana": round(statistics.median(temp_channel2), 2) if temp_channel2 else None
        }
        
        return estadisticas



def publicar_estadisticas():
    estadisticas = calcular_estadisticas()
    if estadisticas:
        decoded_payload = {
            "Temp_Channel1": estadisticas['temp_channel1_promedio'],
            "Temp_Channel2": estadisticas['temp_channel2_promedio']
        }
        frm_payload = base64.b64encode(json.dumps(decoded_payload).encode()).decode()

        uplink_message = {
            "end_device_ids": {
                "device_id": TTN_DEVICE_ID,
                "application_ids": {
                    "application_id": TTN_APP_ID
                },
                "dev_eui": "A8404196BC59BB3B",
                "join_eui": "A840410000000101",
                "dev_addr": "260BE6A7"
            },
            "uplink_message": {
                "f_port": 1,
                "f_cnt": 10,
                "frm_payload": frm_payload,
                "decoded_payload": decoded_payload,
                "rx_metadata": [
                    {
                        "gateway_ids": {
                            "gateway_id": "dragino-dlos8n-gateway",
                            "eui": "A84041FFFF1F65F1"
                        }
                    }
                ]
            }
        }
        
        client = mqtt.Client()
        client.username_pw_set(f"{TTN_APP_ID}@ttn", TTN_ACCESS_KEY)
        client.connect(MQTT_BROKER, MQTT_PORT, 60)

        payload_str = json.dumps(uplink_message)
        result, mid = client.publish(f"v3/{TTN_APP_ID}/devices/{TTN_DEVICE_ID}/up", payload_str)
        
        if result == mqtt.MQTT_ERR_SUCCESS:
            print(f"Estadísticas publicadas en TTN: {payload_str}")
        else:
            print("Error al publicar estadísticas en TTN")

        client.loop_start()
        client.loop_stop()

if __name__ == "__main__":
    publicar_estadisticas()

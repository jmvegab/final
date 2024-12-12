import mysql.connector
import os
import json
import sys
from datetime import datetime
from mysql.connector import Error

# Función para conectarse a MySQL
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

# Crear la base de datos si no existe
def crear_base_datos_si_no_existe(cursor):
    cursor.execute("CREATE DATABASE IF NOT EXISTS mydatabase")
    cursor.execute("USE mydatabase")

# Crear la tabla con los campos correspondientes según el archivo JSON
def crear_tabla_si_no_existe(cursor, nombre_tabla, campos):
    # Asegurarse de que el nombre de la tabla esté entre comillas invertidas
    campos_sql = ', '.join([f"`{campo}` FLOAT" for campo in campos])
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS `{nombre_tabla}` (
        entry_id INT PRIMARY KEY AUTO_INCREMENT,
        created_at DATETIME,
        {campos_sql}
    )
    """)

# Convertir el formato de 'created_at' o 'received_at' de ISO 8601 a 'YYYY-MM-DD HH:MM:SS'
def convertir_formato_fecha(fecha_iso):
    # Si la fecha contiene microsegundos adicionales, recortar para tener solo 6 dígitos de microsegundos
    if 'Z' in fecha_iso:
        fecha_iso = fecha_iso.split('Z')[0]  # Eliminar la 'Z' al final
    if '.' in fecha_iso:
        fecha_iso = fecha_iso[:fecha_iso.index('.') + 7]  # Recortar a 6 dígitos de microsegundos si existen

    # Intentar diferentes formatos para manejar variaciones en las fechas
    formatos = ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"]

    for formato in formatos:
        try:
            fecha_formateada = datetime.strptime(fecha_iso, formato).strftime("%Y-%m-%d %H:%M:%S")
            return fecha_formateada
        except ValueError:
            continue

    # Si todos los formatos fallan, imprimir un mensaje de error
    print(f"Error al convertir la fecha: {fecha_iso}")
    return None

def ejecutar_insert(cursor, nombre_tabla, created_at, campos, valores):
    campos_sql = ', '.join([f"`{campo}`" for campo in campos])
    placeholders = ', '.join(['%s'] * len(campos))
    cursor.execute(f"""
    INSERT INTO `{nombre_tabla}` (created_at, {campos_sql})
    VALUES (%s, {placeholders})
    """, (created_at, *valores))


    
# Función combinada para insertar o actualizar datos en la tabla
def insertar_o_actualizar_datos(cursor, datos, nombre_tabla, campos, is_ttn):
    # Para TTN, iteramos sobre cada objeto en la lista directamente
    if is_ttn:
        for entry in datos:
            created_at_iso = entry.get('received_at')
            created_at = convertir_formato_fecha(created_at_iso)

            if not created_at:
                print(f"Error al convertir la fecha: {created_at_iso}")
                continue

            # Obtener los valores de los campos desde "decoded_payload"
            decoded_payload = entry.get('decoded_payload', {})
            valores = [decoded_payload.get(campo) for campo in campos]

            ejecutar_insert(cursor, nombre_tabla, created_at, campos, valores)



    # Para TSPK, iteramos sobre "feeds" y usamos entry_id
    else:
        for feed in datos['feeds']:
            entry_id = feed['entry_id']
            created_at_iso = feed['created_at']
            created_at = convertir_formato_fecha(created_at_iso)

            if not created_at:
                print(f"Error al convertir la fecha: {created_at_iso}")
                continue

            # Extraer valores específicos de TSPK (field1, field2, etc.)
            temperatura = feed.get('field1')
            humedad = feed.get('field2')
            presion = feed.get('field3')
            calidad_aire = feed.get('field4')
            viento = feed.get('field5')

            # Verificar si la entrada ya existe en la tabla
            cursor.execute(f"SELECT COUNT(*) FROM `{nombre_tabla}` WHERE entry_id = %s", (entry_id,))
            existe = cursor.fetchone()[0]

            if existe:
                # Actualizar datos si ya existe
                cursor.execute(f"""
                UPDATE `{nombre_tabla}` 
                SET created_at = %s, temperatura = %s, humedad = %s, presion = %s, calidad_aire = %s, viento = %s
                WHERE entry_id = %s
                """, (created_at, temperatura, humedad, presion, calidad_aire, viento, entry_id))
            else:
                # Insertar datos si no existe
                cursor.execute(f"""
                INSERT INTO `{nombre_tabla}` (entry_id, created_at, temperatura, humedad, presion, calidad_aire, viento) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (entry_id, created_at, temperatura, humedad, presion, calidad_aire, viento))


# Función principal para procesar el archivo JSON
def procesar_json_a_mysql(nombre_archivo):
    with open(nombre_archivo, 'r') as archivo:
        datos = json.load(archivo)

    # Determinar si es TTN o TSPK basándose en la estructura del archivo JSON
    is_ttn = isinstance(datos, list) and 'received_at' in datos[0] and 'decoded_payload' in datos[0]

    # Extraer nombre de la tabla y campos según el tipo de archivo
    if is_ttn:
        # Para TTN, usa el nombre del dispositivo como nombre de la tabla y los campos de decoded_payload
        nombre_tabla = datos[0]['name']
        campos = list(datos[0]['decoded_payload'].keys())
    else:
        # Para TSPK, usa el nombre en 'channel' y los campos en 'field1', 'field2', etc.
        nombre_tabla = datos['channel']['name']
        campos = []
        for i in range(1, 9):
            campo = datos['channel'].get(f"field{i}")
            if campo:
                campos.append(campo)
            else:
                break

    # Conectar a la base de datos MySQL
    conexion = conectar_mysql()
    cursor = conexion.cursor()

    # Crear la base de datos si no existe y seleccionarla
    crear_base_datos_si_no_existe(cursor)

    # Crear la tabla si no existe
    crear_tabla_si_no_existe(cursor, nombre_tabla, campos)

    # Insertar o actualizar los datos
    insertar_o_actualizar_datos(cursor, datos, nombre_tabla, campos, is_ttn)

    # Confirmar los cambios
    conexion.commit()

    # Cerrar la conexión
    cursor.close()
    conexion.close()
    print(f"Proceso completado y datos guardados/actualizados en la tabla '{nombre_tabla}' en MySQL.")


# Asegúrate de que se está llamando al archivo JSON correcto desde el menú
if __name__ == "__main__":
    if len(sys.argv) > 1:
        archivo_json = sys.argv[1]
        procesar_json_a_mysql(archivo_json)
    else:
        print("Por favor, proporciona un archivo JSON.")
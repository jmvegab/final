import sys
import logging
import os
import time
from datetime import datetime, timezone
from subprocess import call

DEFAULT_BASE_PATH = "/app/data"  
DEFAULT_BASE_PATH_LOGS = "/app/logs"  # Directorio base para almacenar archivos en Docker

from thingspeak import (
    get_data_from_date_backward,
    get_data_from_date_forward,
    fetch_all_data,
    get_data_by_fields,
    get_data_from_date
)
from thethingstack import (
    download_full_history,
    download_between_dates,
    download_from_date,
    download_to_date
)

# Variable global para almacenar el Channel ID
current_channel_id = None

# Configuración del sistema de logging
def setup_logger():
    """Configura el sistema de logging con archivo rotativo diario."""
    log_filename = os.path.join(DEFAULT_BASE_PATH_LOGS, datetime.now().strftime('logs_%Y-%m-%d.log'))
    logging.basicConfig(
        filename=log_filename,
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def log_action(action, level='info'):
    """Registra una acción en el log con un nivel específico."""
    if level == 'info':
        logging.info(action)
    elif level == 'warning':
        logging.warning(action)
    elif level == 'error':
        logging.error(action)
    else:
        logging.debug(action)

def log_exception(e):
    """Registra excepciones o errores."""
    logging.exception(f"Excepción capturada: {e}")

setup_logger()

def mostrar_comandos_disponibles():
    """Muestra los comandos disponibles en el menú."""
    print("\n*** Comandos Disponibles ***")
    print("-set_channel <id>         -> Configurar o cambiar el Channel ID actual")
    print("-tspk -fetch_all          -> Descargar toda la información en ThingSpeak")
    print("-tspk -f <campos>         -> Descargar información de campos específicos en ThingSpeak")
    print("-tspk -d <fecha_ini> <hora_ini> <fecha_fin> <hora_fin> -> Descargar entre dos fechas y horas")
    print("-tspk -dfwd <fecha>       -> Descargar desde una fecha hacia adelante")
    print("-tspk -db <fecha>         -> Descargar desde una fecha hacia atrás")
    print("-ttn -fetch_all           -> Descargar todo el historial de TTN")
    print("-ttn -d <fecha_ini> <fecha_fin> -> Descargar datos entre dos fechas en TTN")
    print("-ttn -dfwd <fecha>        -> Descargar desde una fecha en adelante en TTN")
    print("-ttn -db <fecha>          -> Descargar desde una fecha hacia atrás en TTN")
    print("-logs                     -> Leer el archivo de logs más reciente")
    print("salir                     -> Salir del programa\n")

def format_date_time_input(date_str, time_str):
    """Formatea fecha y hora a ISO 8601 UTC."""
    try:
        dt = datetime.strptime(f"{date_str} {time_str}", "%d-%m-%Y %H:%M:%S")
        dt_utc = dt.replace(tzinfo=timezone.utc)
        return dt_utc.isoformat()
    except ValueError:
        log_action(f"Error en formato de fecha/hora: {date_str} {time_str}", level='error')
        raise ValueError("Formato de fecha u hora no válido. Usa DD-MM-YYYY para la fecha.")

def parse_fields(fields_str):
    """Convierte una cadena de campos en lista."""
    return fields_str.split(',')

def ejecutar_mysql_bot_y_mqtt(nombre_archivo_json):
    ruta_archivo_json = os.path.join(DEFAULT_BASE_PATH, nombre_archivo_json)
    
    if nombre_archivo_json.startswith("TTN_"):
        # Ejecutar los scripts específicos para TTN
        call(['python3', 'mySQL.py', ruta_archivo_json])
        call(['python3', 'botTelegramttn.py', ruta_archivo_json])
        call(['python3', 'publicarMQTTttn.py'])
        call(['python3', 'reporteCalidadttn.py'])

    elif nombre_archivo_json.startswith("TSPK_"):
        # Ejecutar los scripts específicos para ThingSpeak
        call(['python3', 'mySQL.py', ruta_archivo_json])
        call(['python3', 'botTelegramtspk.py', ruta_archivo_json])
        call(['python3', 'publicarMQTTtspk.py'])
        call(['python3', 'reporteCalidadtspk.py'])

    else:
        print(f"Prefijo de archivo desconocido para {nombre_archivo_json}")

def leer_logs():
    """Lee el archivo de log más reciente y vuelve al menú al presionar Ctrl+C."""
    log_filename = os.path.join(DEFAULT_BASE_PATH_LOGS, datetime.now().strftime('logs_%Y-%m-%d.log'))

    if not os.path.exists(log_filename):
        print("No hay archivo de log para el día actual.")
        return

    print(f"Mostrando el contenido de {log_filename}. Presiona Ctrl+C para volver al menú.")
    
    try:
        with open(log_filename, 'r') as log_file:
            for line in log_file:
                print(line, end="")
                time.sleep(0.05)  # Añadir una breve pausa para facilitar la lectura
    except KeyboardInterrupt:
        print("\nRegresando al menú principal...")

def handle_command(args):
    """Procesa los comandos dados por el usuario."""
    global current_channel_id  # Usar la variable global de channel_id

    # Comando para establecer o cambiar el Channel ID
    if len(args) == 2 and args[0].lower() == '-set_channel':
        current_channel_id = args[1]
        print(f"Channel ID configurado como: {current_channel_id}")
        return

    # Salir del programa
    if len(args) == 1 and args[0].lower() == 'salir':
        print("Saliendo del programa...")
        sys.exit()

    # Leer logs
    if len(args) == 1 and args[0].lower() == '-logs':
        leer_logs()
        return

    # Verificar longitud de argumentos
    if len(args) < 2:
        mostrar_comandos_disponibles()
        return

    # Definir la plataforma y el comando
    platform = args[0]
    command = args[1]

    if platform == '-tspk':
        # Prefijo para archivos de ThingSpeak
        prefix = "TSPK_"

        # Verificar que haya un Channel ID configurado
        if not current_channel_id:
            current_channel_id = input("Introduce el Channel ID de ThingSpeak: ")

        if command == '-fetch_all':
            log_action("Comando ejecutado: Descargar toda la información", level='info')
            archivo_generado = f'{prefix}datosRecuperados_all.json'
            fetch_all_data(current_channel_id)
            ejecutar_mysql_bot_y_mqtt(archivo_generado)

        elif command == '-f':
            if len(args) < 3:
                print("Debe proporcionar los campos con el comando `-f`.")
                return
            fields = parse_fields(args[2])
            log_action(f"Comando ejecutado: Descargar información por campos {fields}", level='info')
            field_str = ','.join(fields)
            archivo_generado = f'{prefix}datosRecuperados_field_{field_str}.json'
            get_data_by_fields(current_channel_id, fields)
            ejecutar_mysql_bot_y_mqtt(archivo_generado)

        elif command == '-d':
            if len(args) < 6:
                print("Debe proporcionar fecha de inicio, hora de inicio, fecha de fin y hora de fin con el comando `-d`.")
                print("Ejemplo: -d 01-11-2024 00:00:00 02-11-2024 23:59:59")
                return
            start_date = format_date_time_input(args[2], args[3])
            end_date = format_date_time_input(args[4], args[5])
            log_action(f"Comando ejecutado: Descargar información entre {start_date} y {end_date}", level='info')
            archivo_generado = f'{prefix}datosRecuperados_date_{start_date}_to_{end_date}.json'
            get_data_from_date(current_channel_id, start_date=start_date, end_date=end_date)
            ejecutar_mysql_bot_y_mqtt(archivo_generado)

        elif command == '-dfwd':
            if len(args) < 3:
                print("Debe proporcionar la fecha de inicio con el comando `-dfwd`.")
                print("Ejemplo: -dfwd 01-11-2024")
                return
            start_date = format_date_time_input(args[2], "00:00:00")
            log_action(f"Comando ejecutado: Descargar información desde {start_date} hacia adelante", level='info')
            archivo_generado = f'{prefix}datosRecuperados_forward_{start_date}.json'
            get_data_from_date_forward(current_channel_id, start_date=start_date)
            ejecutar_mysql_bot_y_mqtt(archivo_generado)

        elif command == '-db':
            if len(args) < 3:
                print("Debe proporcionar la fecha de inicio con el comando `-db`.")
                print("Ejemplo: -db 01-11-2024")
                return
            start_date = format_date_time_input(args[2], "00:00:00")
            log_action(f"Comando ejecutado: Descargar información desde {start_date} hacia atrás", level='info')
            archivo_generado = f'{prefix}datosRecuperados_backward_{start_date}.json'
            get_data_from_date_backward(current_channel_id, start_date=start_date)
            ejecutar_mysql_bot_y_mqtt(archivo_generado)

        else:
            print("Comando no reconocido para ThingSpeak.")
            mostrar_comandos_disponibles()

    elif platform == '-ttn':
        # Prefijo para archivos de TTN
        prefix = "TTN_"

        if command == '-fetch_all':
            log_action("Comando ejecutado: Descargar todo el historial de TTN", level='info')
            archivo_generado = f'{prefix}full_history.json'
            download_full_history()
            ejecutar_mysql_bot_y_mqtt(archivo_generado)

        elif command == '-d':
            if len(args) < 4:
                print("Debe proporcionar fecha de inicio y fin con el comando `-d`.")
                print("Ejemplo: -d 2024-11-01 2024-11-02")
                return
            start_date = args[2]
            end_date = args[3]
            log_action(f"Comando ejecutado: Descargar datos entre {start_date} y {end_date} en TTN", level='info')
            archivo_generado = f'{prefix}history_{start_date}_to_{end_date}.json'
            download_between_dates(start_date, end_date)
            ejecutar_mysql_bot_y_mqtt(archivo_generado)

        elif command == '-dfwd':
            if len(args) < 3:
                print("Debe proporcionar la fecha de inicio con el comando `-dfwd`.")
                print("Ejemplo: -dfwd 2024-11-01")
                return
            start_date = args[2]
            log_action(f"Comando ejecutado: Descargar desde {start_date} en adelante en TTN", level='info')
            archivo_generado = f'{prefix}history_from_{start_date}.json'
            download_from_date(start_date)
            ejecutar_mysql_bot_y_mqtt(archivo_generado)

        elif command == '-db':
            if len(args) < 3:
                print("Debe proporcionar la fecha de inicio con el comando `-db`.")
                print("Ejemplo: -db 2024-11-01")
                return
            end_date = args[2]
            log_action(f"Comando ejecutado: Descargar hasta {end_date} en TTN", level='info')
            archivo_generado = f'{prefix}history_to_{end_date}.json'
            download_to_date(end_date)
            ejecutar_mysql_bot_y_mqtt(archivo_generado)

        else:
            print("Comando no reconocido para The Things Network.")
            mostrar_comandos_disponibles()

    else:
        print("Plataforma no reconocida. Usa `-tspk` para ThingSpeak o `-ttn` para The Things Network.")
        mostrar_comandos_disponibles()

def main():
    mostrar_comandos_disponibles()
    while True:
        print("\n*** Menú Principal ***")
        command_input = input("Introduce un comando (o 'salir' para terminar): ").split()
        if 'salir' in command_input:
            print("Saliendo del programa...")
            sys.exit()
        handle_command(command_input)

if __name__ == "__main__":
    main()

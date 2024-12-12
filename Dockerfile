# Usar una imagen base de Python
FROM python:3.12

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar todos los archivos del proyecto al directorio de trabajo del contenedor
COPY . /app

# Instalar dependencias (si tienes un archivo requirements.txt)
RUN pip install -r requirements.txt

# Comando predeterminado para ejecutar el menú principal
CMD ["python3", "menu.py"]

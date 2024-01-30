#Usa la imagen para docker de playwright en python https://playwright.dev/python/docs/docker
FROM mcr.microsoft.com/playwright/python:v1.41.0-jammy

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar todos los archivos del directorio actual al contenedor
COPY . /app

# Instalar virtualenv en el contenedor
RUN pip install virtualenv

# Crear un entorno virtual llamado 'env'
RUN virtualenv env

# Activar el entorno virtual
# Nota: En un Dockerfile, no se puede usar 'source' directamente, se debe ejecutar como un comando de shell
RUN . env/bin/activate

# Instalar dependencias del archivo requirements.txt
RUN pip install -r requirements.txt

# Instalar Playwright
RUN playwright install

# Exponer el puerto 8080
EXPOSE 8080

# Comando para ejecutar el script main.py
CMD ["python", "main.py"]


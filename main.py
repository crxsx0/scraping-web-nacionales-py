import os, construmart, funciones, json, sodimac, logging, easy
from dotenv import load_dotenv
from flask import Flask
from datetime import datetime
import pandas as pd
#carga las variables de entorno
load_dotenv()

# Configurar el nivel de registro global
logging.basicConfig(level=logging.DEBUG)

'''Descomentar si esta en local. Es la KEY con los permisos para usar GCP'''
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'permisos.json'

app = Flask(__name__)
'''Esta funcion ejecuta la funcion asincrona main, se activa haciendo una peticion GET a la URL de Cloud Run'''
@app.route("/")
async def main():
    #Lee el archivo parametros.json
    with open('parametros.json', 'r', encoding='utf-8') as archivo:
        parametros = json.load(archivo)

    #dia(hoy) de la extraccion
    fecha_extraccion = datetime.now()

    #Path para Cloud Storage y Big Query
    nombre_archivo = fecha_extraccion.strftime("%Y%m%d")+".csv"
    nombre_dataset = os.getenv("NOMBRE_DATASET")
    nombre_proyecto= os.getenv("NOMBRE_PROYECTO")
    nombre_bucket = os.getenv("NOMBRE_BUCKET")
    #Path para construmart
    nombre_carpeta_construmart = 'construmart_prueba'
    #Path para sodimac
    nombre_carpeta_sodimac = 'sodimac_prueba'
    #Path para easy
    nombre_carpeta_easy = 'easy_prueba'

    '''Esta funcion usa playwright de manera asincrona para ejecutar el scraping de cada pagina, luego crea el csv a partir de un dataframe lo sube a CLoud Storage y lo sube a Big Query'''
    df_construmart = await construmart.scraping_tiendas(parametros, fecha_extraccion)
    df_sodimac = await sodimac.scraping(fecha_extraccion)
    df_easy = await easy.scraping(fecha_extraccion)
    
    try:
        #Sube dataframe de Construmart a GCP
        funciones.subir_dataframe_cloud(df_construmart, nombre_proyecto, nombre_bucket, nombre_dataset, nombre_carpeta_construmart, nombre_archivo, parametros["construmartTabla"])
        #Sube dataframe de Sodimac a GCP
        funciones.subir_dataframe_cloud(df_sodimac, nombre_proyecto, nombre_bucket, nombre_dataset, nombre_carpeta_sodimac, nombre_archivo, parametros["sodimacTabla"])
        #Sube dataframe de Easy a GCP
        funciones.subir_dataframe_cloud(df_easy, nombre_proyecto, nombre_bucket, nombre_dataset, nombre_carpeta_easy, nombre_archivo, parametros["easyTabla"])
        return 'Archivo CSV y DataFrame subido exitosamente.'
    except Exception as e:
        return f'Hubo un error: {str(e)}'
    

'''Configuraciones para el servidor de flask'''
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

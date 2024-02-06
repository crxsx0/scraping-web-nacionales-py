import os, construmart, funciones, json
from playwright.async_api import async_playwright
from dotenv import load_dotenv
from flask import Flask
from datetime import datetime
import pandas as pd
#carga las variables de entorno
load_dotenv()

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
    nombre_bucket = os.getenv("NOMBRE_BUCKET")
    nombre_dataset = os.getenv("NOMBRE_DATASET")
    nombre_proyecto = os.getenv("NOMBRE_PROYECTO")
    nombre_carpeta = 'construmart'
    
    '''Esta funcion usa playwright de manera asincrona para ejecutar el scraping de cada pagina, luego crea el csv a partir de un dataframe lo sube a CLoud Storage y lo sube a Big Query'''
    async with async_playwright() as p:
        df_construmart = await construmart.scraping_tiendas(p, parametros, fecha_extraccion)
        df_csv = df_construmart.to_csv(index= False)
        ruta_csv = f'{nombre_carpeta}/{nombre_archivo}'
        ruta_dfbq = f'{nombre_proyecto}.{nombre_dataset}.construmart'
        df_construmart['fecha'] = pd.to_datetime(df_construmart['fecha'])
        
        try:
            funciones.existencia_dataset_tabla(nombre_proyecto, nombre_dataset, nombre_carpeta, parametros)
            #Se sube el dataframe en archivo csv a Cloud Storage
            funciones.subir_dfcsv_cstorage(df_csv, ruta_csv, nombre_bucket)
            #Se sube el dataframe a BigQuery
            funciones.subir_dataframe_bq(df_construmart, ruta_dfbq)
            return 'Archivo CSV y DataFrame subido exitosamente.'
        except Exception as e:
            return f'Hubo un error: {str(e)}'

'''Configuraciones para el servidor de flask'''
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

import os, construmart, funciones
from playwright.async_api import async_playwright
from flask import Flask
from datetime import datetime
import pandas as pd

app = Flask(__name__)
'''Esta funcion ejecuta la funcion asincrona main, se activa haciendo una peticion GET a la URL de Cloud Run'''
@app.route("/")
async def main():
    #dia(hoy) de la extraccion
    fecha_extraccion = datetime.now()

    #Path para Cloud Storage y Big Query
    nombre_archivo = fecha_extraccion.strftime("%Y%m%d")+".csv"
    nombre_bucket = 'cmpc-datos-precios-madera'
    nombre_carpeta = 'construmart'
    nombre_tabla = 'cmpc-datos-soluciones-sandbox.precios_madera.construmart'
    
    '''Esta funcion usa playwright de manera asincrona para ejecutar el scraping de cada pagina, luego crea el csv a partir de un dataframe lo sube a CLoud Storage y lo sube a Big Query'''
    async with async_playwright() as p:
        df_construmart = await construmart.scraping(p,'XIII REGIÓN METROPOLITANA DE SANTIAGO', 'LAS CONDES', fecha_extraccion)
        df_csv = df_construmart.to_csv(index= False)
        ruta_csv = f'{nombre_carpeta}/{nombre_archivo}'
        df_construmart['Fecha'] = pd.to_datetime(df_construmart['Fecha'])
        
        try:
            #Se sube el dataframe en archivo csv a Cloud Storage
            funciones.subir_dfcsv_cstorage(df_csv, ruta_csv, nombre_bucket)
            #Se sube el dataframe a BigQuery
            funciones.subir_dataframe_bq(df_construmart, nombre_tabla)
            return 'Archivo CSV y DataFrame subido exitosamente.'
        except Exception as e:
            return f'Hubo un error: {str(e)}'

'''Configuraciones para el servidor de flask'''
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

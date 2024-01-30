import os
import asyncio
from playwright.async_api import async_playwright
from flask import Flask
import construmart
from datetime import datetime
import pandas as pd
import funciones

async def main():
    fecha_extraccion = datetime.now()

    nombre_archivo = fecha_extraccion.strftime("%Y%m%d")+".csv"
    nombre_bucket = 'cmpc-datos-precios-madera'
    nombre_carpeta = 'construmart'
    nombre_tabla = 'cmpc-datos-soluciones-sandbox.precios_madera.construmart'

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cmpc-datos-soluciones-sandbox-d07dd883290d.json'
    
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
            return print('Archivo CSV y DataFrame subido exitosamente.')
        except Exception as e:
            return print(f'Hubo un error: {str(e)}')

app = Flask(__name__)

@app.route("/")
async def main():
    fecha_extraccion = datetime.now()

    nombre_archivo = fecha_extraccion.strftime("%Y%m%d")+".csv"
    nombre_bucket = 'cmpc-datos-precios-madera'
    nombre_carpeta = 'construmart'
    nombre_tabla = 'cmpc-datos-soluciones-sandbox.precios_madera.construmart'

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cmpc-datos-soluciones-sandbox-d07dd883290d.json'
    
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

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

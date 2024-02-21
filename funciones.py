import pandas_gbq, unicodedata, logging
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

'''Esta funcion se encarga de subir el dataframe a la tabla correspondiente en BigQuery. Es importante tener instalada la libreria de pandas_gbq. La cuenta de servicio que se utilizara para ejecutar tiene que tener permisos para usar BigQuery'''
def subir_dataframe_bq(dataframe, ruta_tabla):
    try:
        pandas_gbq.to_gbq(dataframe, ruta_tabla, if_exists= 'append')
        return logging.info('El dataframe se subio exitosamente a BigQuery\n')
    except Exception as e:
        return logging.error('Hubo un error al subir el dataframe a BigQuery\n' + str(e) + '\n')

'''Esta funcion se encarga de subir el dataframe convertido a csv a Cloud Storage. Para esta funcion se utiliza la libreria google-cloud-storage. Tambien requiere de que la cuenta de servicio con la que se ejecutara tenga permisos para usar Cloud Storage'''
def subir_dfcsv_cstorage( dataframe_csv, ruta_destino_csv,  nombre_bucket):
    #crea un cliente de cloud storage
    cliente_storage = storage.Client()
    bucket = cliente_storage.get_bucket(nombre_bucket)
    blob = bucket.blob(ruta_destino_csv)
    
    try:
        blob.upload_from_string(dataframe_csv, content_type='text/csv')
        return logging.info('Se subio el csv exitosamente a Cloud Storage\n')
    except Exception as e:
        return logging.error('Hubo un error al subir el csv a Cloud Storage\n'+ str(e) + '\n')
def quitar_tildes(texto):
    """
    Elimina las tildes de un texto.
    :param texto: El texto al que se le quieren quitar las tildes.
    :return: El texto sin tildes.
    """
    texto_sin_tildes = ''.join(
        (c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    )
    return texto_sin_tildes
'''Hace scroll hasta que termina que la cantidad de datos mostrados en la pagina sea igual a la cantidad de elementos totales '''
async def scroll_infinito(page, selector, cantidad):
    while True:
        #Ejecuta codigo de javascript para saber que cantidad esta mostrando la pgina
        elemento = await page.evaluate('''selector => {
            return document.querySelector(selector).innerText;
        }''', selector);
        if elemento == cantidad:
            break
        await espera_elementos_pantalla(page,".vtex-search-result-3-x-showingProductsCount" ,".vtex-store-components-3-x-productBrandName")
async def espera_elementos_pantalla(page, selector_cantidad, selector_cards):
    while True:
        await page.evaluate('''async () => {
            const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
            for (let i = 0; i < document.body.scrollHeight; i += 100) {
                window.scrollTo(0, i);
                await delay(150);
            }
        }''')
        cantidad_elementos_pantalla = await page.evaluate('''selector => {
            return document.querySelector(selector).innerText.split(' ')[0]
        };
        ''', selector_cantidad)
        cantidad_cards = await page.evaluate('''selector => {
            return document.querySelectorAll(selector).length
        };
        ''', selector_cards)
        if int(cantidad_elementos_pantalla) == cantidad_cards:
            break
'''Extrae elementos con codigo de javascript y retorna una lista con estos'''
async def extraer_lista_elementos_texto(page, selector):
    lista = await page.evaluate('''selector => {
            const elementos = Array.from(document.querySelectorAll(selector));
            return elementos.map(elemento => elemento.innerText);
        }''', selector);
    return lista
'''Extrae links y retorna una lista con los links'''
async def extraer_lista_links(page, selector):
    lista = await page.evaluate('''selector => {
            const elementos = Array.from(document.querySelectorAll(selector));
            return elementos.map(elemento => elemento.href);
        }''', selector)
    return lista
'''Limpia un string precio, y retorna el precio como un entero"'''
def limpiar_formato_moneda(precio):
    # Elimina el símbolo de moneda y convierte a número
    return int(precio.replace('$', '').replace('.', ''))
async def extraer_cantidad_paginas(page, selector):
    lista = await page.evaluate('''selector => {
            const elementos = Array.from(document.querySelectorAll(selector));
            return elementos.map(elemento => elemento.innerText).at(-1);
        }''', selector);
    return lista
def creacion_tabla_bq (id_tabla, parametros):
    try:
        client = bigquery.Client()
        schema = parametros['tableSchema']
        tabla = bigquery.Table(id_tabla, schema=schema)
        tabla.labels = parametros["labels"]
        tabla.description = parametros["descripcionTabla"]
        tabla.clustering_fields = ["fecha"]
        tabla.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="fecha",
        )
        tabla = client.create_table(tabla)
    except Exception as e:
        return logging.error(f"hubo un error: {e}")
def existencia_dataset_tabla(id_proyecto, id_dataset, id_tabla, parametros):
    client = bigquery.Client(project=id_proyecto)
    dataset_full_id = f"{id_proyecto}.{id_dataset}"
    table_full_id = f"{dataset_full_id}.{id_tabla}"
    # Verificar y crear el dataset si es necesario
    try:
        client.get_dataset(id_dataset)  # Intenta obtener el dataset
        logging.info(f"El dataset {dataset_full_id} ya existe.")
    except NotFound:
        logging.warning(f"El dataset {dataset_full_id} no existe, creándolo...")
        dataset = bigquery.Dataset(dataset_full_id)
        dataset.location = "US"  # Especifica la ubicación según sea necesario
        client.create_dataset(dataset)
        logging.error(f"Dataset {dataset_full_id} creado con éxito.")
    # Verificar y crear la tabla si es necesario
    try:
        client.get_table(table_full_id)  # Intenta obtener la tabla
        logging.info(f"La tabla {table_full_id} ya existe.")
    except NotFound:
        logging.warning(f"La tabla {table_full_id} no existe, creándola...")
        creacion_tabla_bq(table_full_id, parametros)
        logging.error(f"Tabla {table_full_id} creada con éxito.")
def subir_dataframe_cloud(df_pagina, nombre_proyecto, nombre_bucket, nombre_dataset, nombre_carpeta, nombre_archivo, parametros):
    df_csv = df_pagina.to_csv(index= False)
    ruta_csv = f'{nombre_carpeta}/{nombre_archivo}'
    ruta_dfbq = f'{nombre_proyecto}.{nombre_dataset}.{nombre_carpeta}'
    #df_pagina['fecha'] = pd.to_datetime(df_pagina['fecha'])
    try:
        existencia_dataset_tabla(nombre_proyecto, nombre_dataset, nombre_carpeta, parametros)
        #Se sube el dataframe en archivo csv a Cloud Storage
        subir_dfcsv_cstorage(df_csv, ruta_csv, nombre_bucket)
        #Se sube el dataframe a BigQuery
        subir_dataframe_bq(df_pagina, ruta_dfbq)
        return logging.info('Archivo CSV y DataFrame subido exitosamente.')
    except Exception as e:
        return e
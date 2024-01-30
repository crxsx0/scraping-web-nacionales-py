import pandas_gbq
import unicodedata
from google.cloud import storage

'''Esta funcion se encarga de subir el dataframe a la tabla correspondiente en BigQuery. Es importante tener instalada la libreria de pandas_gbq. La cuenta de servicio que se utilizara para ejecutar tiene que tener permisos para usar BigQuery'''
def subir_dataframe_bq(dataframe, ruta_tabla):
    try:
        pandas_gbq.to_gbq(dataframe, ruta_tabla, if_exists= 'append')
        return print('El dataframe se subio exitosamente a BigQuery\n')
    except Exception as e:
        return print('Hubo un error al subir el dataframe a BigQuery\n' + str(e) + '\n')


'''Esta funcion se encarga de subir el dataframe convertido a csv a Cloud Storage. Para esta funcion se utiliza la libreria google-cloud-storage. Tambien requiere de que la cuenta de servicio con la que se ejecutara tenga permisos para usar Cloud Storage'''

def subir_dfcsv_cstorage( dataframe_csv, ruta_destino_csv,  nombre_bucket):
    #crea un cliente de cloud storage
    cliente_storage = storage.Client()
    bucket = cliente_storage.get_bucket(nombre_bucket)
    blob = bucket.blob(ruta_destino_csv)

    try:
        blob.upload_from_string(dataframe_csv, content_type='text/csv')
        return print('Se subio el csv exitosamente a Cloud Storage\n')
    except Exception as e:
        return print('Hubo un error al subir el csv a Cloud Storage\n')
    
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

async def scroll_infinito(page, selector, cantidad):
    while True:
        elemento = await page.evaluate('''selector => {
            return document.querySelector(selector).innerText;
        }''', selector);

        if elemento == cantidad:
            break

        await page.keyboard.press('End')
        await page.wait_for_timeout(1000)

async def extraer_lista_elementos_texto(page, selector):
    lista = await page.evaluate('''selector => {
            const elementos = Array.from(document.querySelectorAll(selector));
            return elementos.map(elemento => elemento.innerText);
        }''', selector);
    return lista
    
async def extraer_lista_links(page, selector):
    lista = await page.evaluate('''selector => {
            const elementos = Array.from(document.querySelectorAll(selector));
            return elementos.map(elemento => elemento.href);
        }''', selector)
    return lista

def limpiar_formato_moneda(precio):
    # Elimina el símbolo de moneda y convierte a número
    return int(precio.replace('$', '').replace('.', ''))

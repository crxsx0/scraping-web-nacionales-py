# Importa las bibliotecas necesarias para el scraping web y el manejo de datos.
from playwright.async_api import async_playwright  # Para automatización web asíncrona.
from playwright_stealth import stealth_async  # Para evadir mecanismos anti-bot.
import pandas as pd  # Para la manipulación y análisis de datos.
import funciones  # Módulo personalizado con funciones de ayuda para el scraping.
import time  # Para realizar pausas durante el scraping.
import logging #Para observar logs

# Función asíncrona para esperar hasta que la URL de la página cambie.
async def cambio_url(page, URL):
    while True:
        try:
            # Obtiene la URL actual de la página y la compara con la esperada.
            url_actual = await page.evaluate('''() => {
                return location.href;
            }''')
            if url_actual != URL:
                logging.info('La URL ya cambió.')
                break  # Si la URL ha cambiado, sale del bucle.
            else:
                logging.warning('La URL aún no ha cambiado, intentando de nuevo...')
                time.sleep(1)  # Espera un segundo antes de volver a comprobar.
        except Exception as e:
            # Captura y muestra cualquier excepción durante la comprobación.
            logging.error(f"Se produjo un error al obtener la URL actual, intentando de nuevo... Error: {e}")
            time.sleep(1)  # Espera un segundo antes de reintentar.

# Función asíncrona para hacer scraping de los detalles de un producto.
async def scraping_producto(page, fecha, nombre_categoria):
    # Define selectores CSS para extraer marca, descripción y precio del producto.
    selector_marca = 'a.product-brand-link'
    selector_descripcion = 'h1'
    selector_precio = 'div.cmr-icon-container span.primary'

    # Utiliza funciones del módulo personalizado para extraer texto de los elementos.
    marcas = await funciones.extraer_lista_elementos_texto(page, selector_marca)
    descripciones = await funciones.extraer_lista_elementos_texto(page, selector_descripcion)
    precios = await funciones.extraer_lista_elementos_texto(page, selector_precio)
    
    # Limpia y formatea los datos extraídos.
    precios_limpio = [funciones.limpiar_formato_moneda(precio) for precio in precios]
    descripcion_sin_tildes = [funciones.quitar_tildes(descripcion) for descripcion in descripciones]
    marcas_sin_tildes = [funciones.quitar_tildes(marca) for marca in marcas]
    url_pagina = await page.evaluate('''() => {
                return location.href;
            }''')
    
    # Crea un DataFrame con los datos limpios y formateados.
    df = pd.DataFrame({
        'fecha': fecha.strftime("%Y-%m-%d"),
        'link': url_pagina,
        'tienda': 'Sodimac',
        'precio': precios_limpio,
        'marca' : marcas_sin_tildes,
        'descripcion': descripcion_sin_tildes,
        'categoria': nombre_categoria
    })

    return df  # Retorna el DataFrame con la información del producto.

# Función asíncrona para hacer scraping de la página principal de productos.
async def scraping_pagina_principal(page, fecha):
    # Espera a que ciertos elementos de la página se carguen antes de proceder.
    await page.locator('div.loader').wait_for(state='detached', timeout=15000)
    time.sleep(2)  # Pausa para asegurar la carga completa de la página.
    
    # Espera a que el selector específico esté presente en la página.
    await page.wait_for_selector('#testId-searchResults', timeout=50000)
    await page.locator("div.search-results-4-grid").first.wait_for(state='visible')
    
    # Define selectores CSS para extraer datos relevantes de la página.
    selector_precio = 'ol.pod-prices li.prices-0 div span.primary.normal';
    selector_descripcion = 'b.pod-subTitle.subTitle-rebrand'
    selector_links = 'div.grid-pod a'
    selector_cantidad = 'div.vtex-search-result-3-x-totalProducts--layout span'
    selector_marca = 'b.pod-title.title-rebrand'
    
    # Extrae la cantidad de productos disponibles usando JavaScript.
    cantidad = await page.evaluate('''selector => {
    const elemento = document.querySelector(selector);
        if (elemento) {
            const textoCompleto = elemento.textContent || '';
            const numero = textoCompleto.split(' ')[0];
            return numero.trim();
        }
        return null;}''', selector_cantidad)
    
    # Extrae datos de los productos usando funciones del módulo personalizado.
    links = await funciones.extraer_lista_links(page, selector_links);
    precios = await funciones.extraer_lista_elementos_texto(page, selector_precio)
    descripciones = await funciones.extraer_lista_elementos_texto(page, selector_descripcion)
    marcas = await funciones.extraer_lista_elementos_texto(page, selector_marca)
    categoria = await funciones.extraer_lista_elementos_texto(page, "h1")
    
    # Ajusta los datos extraídos para asegurar consistencia.
    max_length = max(len(links), len(precios), len(descripciones))
    precios.extend(['$0'] * (max_length - len(precios)))
    
    # Limpia y formatea los datos extraídos.
    precios_limpio = [funciones.limpiar_formato_moneda(precio) for precio in precios]
    descripcion_sin_tildes = [funciones.quitar_tildes(descripcion) for descripcion in descripciones]
    marcas_sin_tildes = [funciones.quitar_tildes(marca) for marca in marcas]
    print(f"Cantidad de links:{len(links)}, precios: {len(precios_limpio)}, marcas: {len(marcas_sin_tildes)}, descripciones: {len(descripcion_sin_tildes)}")
    
    # Crea un DataFrame con los datos limpios y formateados.
    df = pd.DataFrame({
        'fecha': fecha.strftime("%Y-%m-%d"),
        'link': links,
        'tienda': 'Sodimac',
        'precio': precios_limpio,
        'marca' : marcas_sin_tildes,
        'descripcion': descripcion_sin_tildes,
        'categoria': categoria[0]
    })
    
    return df  # Retorna el DataFrame con la información recopilada.

# Función asíncrona para gestionar el scraping de múltiples páginas.
async def scraping_paginas(page, fecha):
    df_final = pd.DataFrame()
    try:
        await page.wait_for_load_state('load')  # Espera a que la página se cargue completamente.
        # Verifica si el botón de paginación está visible y habilitado.
        await page.locator('#testId-pagination-top-arrow-right > i').is_visible(timeout=6000)
        if await page.locator('#testId-pagination-top-arrow-right').is_enabled(timeout=15000):
            # Extrae el número total de páginas y realiza el scraping en cada una.
            cantidad_paginas = int(await funciones.extraer_cantidad_paginas(page, 'div.action-bar ol li button'))
            for i in range(cantidad_paginas):
                df_temporal = await scraping_pagina_principal(page, fecha)
                df_final = pd.concat([df_final, df_temporal])
                if i != cantidad_paginas-1:
                    await page.click("#testId-pagination-top-arrow-right > i")
        else:
            # Si no hay paginación, hace scraping de la página actual.
            df_temporal = await scraping_pagina_principal(page, fecha)
            df_final = pd.concat([df_final, df_temporal])
    except:
        if await page.locator("#NE-2").is_visible(timeout=3000):
            logging.error("Error en la pagina")
        else:
            logging.error("Error al cargar la pagina")
            await page.reload()  # Recarga la página en caso de error.
            await scraping_paginas(page, fecha)  # Intenta hacer scraping nuevamente.

    return df_final

# Función para determinar si se debe hacer scraping de un solo elemento o de múltiples elementos.
async def page_elements_or_element(page, fecha, categoria, df_final):
    if await page.locator("#product-b2c-ui").is_visible(timeout=10000):
        print("Solo hay un elemento")
        df_temporal = await scraping_producto(page, fecha, categoria)
    else:
        print("Es visible")
        df_temporal = await scraping_paginas(page, fecha)  # Asumo que faltaba pasar categoria aquí.

    # Devuelve el dataframe final actualizado
    return pd.concat([df_final, df_temporal])

# Función principal para iniciar el proceso de scraping.
async def scraping(fecha):
    df_final = pd.DataFrame()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless = True)  # Inicia el navegador en modo sin cabeza.
        page = await browser.new_page()  # Abre una nueva página en el navegador.
        await stealth_async(page)  # Aplica técnicas para evitar ser detectado como bot.
        await page.goto("https://sodimac.falabella.com/sodimac-cl/category/CATG10732/Maderas-y-tableros")  # Navega a la URL especificada.
        await page.wait_for_load_state('load')
        await page.wait_for_selector('#testId-searchResults', timeout=50000)  # Espera a que el selector esté presente.
        
        # Extrae los identificadores de las categorías de productos.
        id_categorias = await page.evaluate('''selector => {
            const links = Array.from(document.querySelectorAll(selector));
            return links.map(elemento => elemento.id)
        }
        ''', "ul.singleselect li button")
        
        # Realiza el scraping para cada categoría de productos.
        for id in id_categorias:
            id_selector = "#" + id.replace(" ", "\\ ")
            nombre_categoria = await funciones.extraer_lista_elementos_texto(page, id_selector)
            logging.info(nombre_categoria)
            await page.click(id_selector)
            await page.locator('div.loader').wait_for(state='detached', timeout=15000)
            
            # Verifica si hay resultados para la categoría seleccionada.
            if await page.locator("div.no-result.error-page").is_visible(timeout=3000):
                logging.error("Pagina de error")
                await page.reload()  # Recarga la página si no hay resultados.
                df_final = await page_elements_or_element(page, fecha, nombre_categoria, df_final)
            else:
                df_final = await page_elements_or_element(page, fecha, nombre_categoria, df_final)
                
            # Navega de nuevo a la página principal de categorías entre cada iteración.
            try:
                await page.goto("https://sodimac.falabella.com/sodimac-cl/category/CATG10732/Maderas-y-tableros", timeout=60000)
                await page.wait_for_load_state('load')
            except:
                await page.goto("https://sodimac.falabella.com/sodimac-cl/category/CATG10732/Maderas-y-tableros", timeout=60000)
                await page.wait_for_load_state('load')
        await browser.close()  # Cierra el navegador una vez completado el scraping.

    return df_final  # Retorna el DataFrame final con todos los datos recopilados.

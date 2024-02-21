# Importa las bibliotecas necesarias para el scraping web y el manejo de datos.
from playwright.async_api import async_playwright  # Para automatización web asíncrona.
from playwright_stealth import stealth_async  # Para evadir mecanismos anti-bot.
import pandas as pd  # Para la manipulación y análisis de datos.
import funciones  # Módulo personalizado con funciones de ayuda para el scraping.
import time  # Para realizar pausas durante el scraping.
import logging #Para observar logs
import asyncio
from datetime import datetime

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
async def scraping_pagina_principal(page, fecha, nombre_categoria):
    # Espera a que ciertos elementos de la página se carguen antes de proceder.
    #await page.locator('div.loader').wait_for(state='detached', timeout=15000)
    time.sleep(4)  # Pausa para asegurar la carga completa de la página.
    # Espera a que el selector específico esté presente en la página.
    await page.wait_for_selector('#gallery-layout-container', timeout=50000)
    await page.locator("div.easycl-search-result-0-x-galleryItem ").first.wait_for(state='visible')

    await page.evaluate('''async () => {
            const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
            for (let i = 0; i < document.body.scrollHeight; i += 100) {
                window.scrollTo(0, i);
                await delay(150);
            }
    }''')
    
    # Define selectores CSS para extraer datos relevantes de la página.
    selector_precio = 'div.easycl-precio-cencosud-0-x-lastPrice';
    selector_descripcion = 'span.vtex-product-summary-2-x-productBrand'
    selector_links = 'section.vtex-product-summary-2-x-container a'
    selector_marca = 'div.vtex-store-components-3-x-productBrandContainer'
    
    # Extrae datos de los productos usando funciones del módulo personalizado.
    links = await funciones.extraer_lista_links(page, selector_links);
    precios = await funciones.extraer_lista_elementos_texto(page, selector_precio)
    descripciones = await funciones.extraer_lista_elementos_texto(page, selector_descripcion)
    marcas = await funciones.extraer_lista_elementos_texto(page, selector_marca)
    
    # Ajusta los datos extraídos para asegurar consistencia.
    max_length = max(len(links), len(precios), len(descripciones))
    precios.extend(['$0'] * (max_length - len(precios)))
    
    # Limpia y formatea los datos extraídos.
    precios_limpio = [funciones.limpiar_formato_moneda(precio) for precio in precios]
    descripcion_sin_tildes = [funciones.quitar_tildes(descripcion) for descripcion in descripciones]
    marcas_sin_tildes = [funciones.quitar_tildes(marca) for marca in marcas]
    logging.info(f"Cantidad de links:{len(links)}, precios: {len(precios_limpio)}, marcas: {len(marcas_sin_tildes)}, descripciones: {len(descripcion_sin_tildes)}")
    
    # Crea un DataFrame con los datos limpios y formateados.
    df = pd.DataFrame({
        'fecha': fecha.strftime("%Y-%m-%d"),
        'link': links,
        'tienda': 'Easy',
        'precio': precios_limpio,
        'marca' : marcas_sin_tildes,
        'descripcion': descripcion_sin_tildes,
        'categoria': nombre_categoria
    })
    
    return df  # Retorna el DataFrame con la información recopilada.

# Función asíncrona para gestionar el scraping de múltiples páginas.
async def scraping_paginas(page, fecha, nombre_categoria):
    df_final = pd.DataFrame()

    while True:
        await page.wait_for_load_state('load')  # Espera a que la página se cargue completamente.
        # Verifica si el botón de paginación está visible y habilitado.
        await page.locator('ul.easycl-custom-blocks-4-x-customPagination__items').is_visible(timeout=6000)
        if await page.locator('li.easycl-custom-blocks-4-x-customPagination__next a').is_visible(timeout=15000):
            df_temporal = await scraping_pagina_principal(page, fecha, nombre_categoria)
            df_final = pd.concat([df_final, df_temporal])
            await page.click("li.easycl-custom-blocks-4-x-customPagination__next a")
        else:
            # Si no hay paginación, hace scraping de la página actual.
            df_temporal = await scraping_pagina_principal(page, fecha, nombre_categoria)
            df_final = pd.concat([df_final, df_temporal])
            break

    return df_final

# Función principal para iniciar el proceso de scraping.
async def scraping(fecha):
    df_final = pd.DataFrame()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless = True)  # Inicia el navegador en modo sin cabeza.
        page = await browser.new_page()  # Abre una nueva página en el navegador.
        await stealth_async(page)  # Aplica técnicas para evitar ser detectado como bot.
        await page.goto("https://www.easy.cl/materiales-de-construccion/maderas-y-tableros")  # Navega a la URL especificada.
        await page.wait_for_load_state('load')
        await page.wait_for_selector('#gallery-layout-container', timeout=50000)  # Espera a que el selector esté presente.
        await page.locator('body > div.render-container.render-route-store-search-category > div > div.vtex-store__template.bg-base > div > div:nth-child(6) > div > div > section > div.relative.justify-center.flex > div > div.vtex-flex-layout-0-x-flexRow.vtex-flex-layout-0-x-flexRow--searchResult > div > div.pr5.vtex-flex-layout-0-x-stretchChildrenWidth.flex > div > div:nth-child(3) > div > div > div.easycl-search-result-0-x-filter__container.bb.b--muted-4.easycl-search-result-0-x-filter__container--tipo-de-producto').wait_for(state='visible', timeout=15000)

        selector_categorias = 'div.easycl-search-result-0-x-filter__container--tipo-de-producto div.easycl-search-result-0-x-filterTemplateOverflow div div div div div input'

        # Extrae los identificadores de las categorías de productos.
        id_categorias = await page.evaluate('''selector => {
            const links = Array.from(document.querySelectorAll(selector));
            return links.map(elemento => elemento.id)
        }
        ''', selector_categorias)
        
        nombres_categorias = await page.evaluate('''selector => {
            const links = Array.from(document.querySelectorAll(selector));
            return links.map(elemento => elemento.name)
        }
        ''', selector_categorias)
        print(id_categorias)
        # Realiza el scraping para cada categoría de productos.
        for i in range(len(id_categorias)):
            nombre_categoria = nombres_categorias[i]
            logging.info(nombre_categoria)
            print(nombre_categoria)
            
            id_limpio_url = id_categorias[i].replace("tipo-de-producto-", "")

            url_temporal = f'https://www.easy.cl/materiales-de-construccion/maderas-y-tableros/{id_limpio_url}?initialMap=c,c&initialQuery=materiales-de-construccion/maderas-y-tableros&map=category-1,category-2,tipo-de-producto'

            await page.goto(url_temporal)

            df_temporal = await scraping_paginas(page, fecha, nombre_categoria)
            df_final = pd.concat([df_final, df_temporal])

        await browser.close()  # Cierra el navegador una vez completado el scraping.

    return df_final  # Retorna el DataFrame final con todos los datos recopilados.

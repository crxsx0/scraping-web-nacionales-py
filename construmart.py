from playwright.async_api import async_playwright
import pandas as pd
import funciones

async def scraping(p, region, comuna, fecha):
    async with async_playwright() as p:
        '''Inicia un navegador chromium, crea una pagina y luego va a la URL especificada, espera a ciertos selectores para luego elegir que region y comuna va a consultar'''
        browser = await p.chromium.launch(headless= True)
        page = await browser.new_page()
        await page.goto("https://www.construmart.cl/maderas-y-tableros")
        await page.wait_for_selector('.construmartcl-custom-apps-0-x-modalContent', timeout=50000)
        await page.wait_for_selector('.form-group', timeout= 50000);
        await page.select_option('#region', region)
        await page.select_option('#tienda', comuna)
        await page.click('.storeSelectorButton')

        #Selectores de cada etiqueta HTML a utilizar
        selector_precio = 'span.vtex-product-price-1-x-sellingPrice>span.vtex-product-price-1-x-sellingPriceValue--summary';
        selector_descripcion = '.vtex-product-summary-2-x-productNameContainer'
        selector_links = 'a.vtex-product-summary-2-x-clearLink'
        selector_carga = 'span.vtex-search-result-3-x-showingProductsCount.b'
        selector_cantidad = 'div.vtex-search-result-3-x-totalProducts--layout span'
        selector_marca = 'span.vtex-store-components-3-x-productBrandName'

        #Esta variable ejecuta codigo de javascript en el navegador para extraer la cantidad de elementos que hay en la pagina
        cantidad = await page.evaluate('''selector => {
        const elemento = document.querySelector(selector);
            if (elemento) {
                const textoCompleto = elemento.textContent || '';
                const numero = textoCompleto.split(' ')[0];
                return numero.trim();
            }
            return null;}''', selector_cantidad)
        
        #Espera a selectores para seguir con la ejecucion
        await page.wait_for_selector('strong.construmartcl-custom-apps-0-x-triggerSelectedStore');
        await page.wait_for_selector('#gallery-layout-container')
        
        #Scrollea hasta el final de la pagina para ver todos los elementos
        await funciones.scroll_infinito(page, selector_carga, cantidad)
        await page.wait_for_selector('#gallery-layout-container')

        #Listas con cada uno de los elementos a guardar en el dataframe extraidos desde el html
        links = await funciones.extraer_lista_links(page, selector_links);
        precios = await funciones.extraer_lista_elementos_texto(page, selector_precio)
        descripciones = await funciones.extraer_lista_elementos_texto(page, selector_descripcion)
        marcas = await funciones.extraer_lista_elementos_texto(page, selector_marca)
        #Los precios que estan en NULL los convierte en $0
        max_length = max(len(links), len(precios), len(descripciones))
        precios.extend(['$0'] * (max_length - len(precios)))
        #Limpia la lista precios y las descripciones
        precios_limpio = [funciones.limpiar_formato_moneda(precio) for precio in precios]
        descripcion_sin_tildes = [funciones.quitar_tildes(descripcion) for descripcion in descripciones]
        marcas_sin_tildes = [funciones.quitar_tildes(marca) for marca in marcas]

        #Crea el dataframe con cada una de las columnas
        df = pd.DataFrame({
            'fecha': fecha.strftime("%Y-%m-%d"),
            'link': links,
            'tienda': 'Construmart',
            'precio': precios_limpio,
            'marca' : marcas_sin_tildes,
            'descripcion': descripcion_sin_tildes,
            'region': funciones.quitar_tildes(region),
            'comuna': funciones.quitar_tildes(comuna)
        })
        #Cierra el navegador
        await browser.close()
        #Retorna el dataframe
        return df

'''Esta funcion se encarga de ejecutar el scraping en cada una de las tiendas especificadas en parametros.json y retorna el dataframe final de construmart'''
async def scraping_tiendas(p, tiendas, fecha):
    df_final = pd.DataFrame()

    for tienda in tiendas['tiendas']:
        region = tienda['region']
        comuna = tienda['comuna']
        print(f"{region}, {comuna}")
        df_temporal = await scraping(p, region, comuna, fecha)
        df_final = pd.concat([df_final, df_temporal])
        
    return df_final

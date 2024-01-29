from playwright.async_api import async_playwright
import pandas as pd
import funciones

async def scraping(p, region, comuna, fecha):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless= True)
        page = await browser.new_page()
        await page.goto("https://www.construmart.cl/maderas-y-tableros")
        await page.wait_for_selector('.construmartcl-custom-apps-0-x-modalContent', timeout=50000)
        await page.wait_for_selector('.form-group', timeout= 50000);
        await page.select_option('#region', region)
        await page.select_option('#tienda', comuna)
        await page.click('.storeSelectorButton')
        
        selector_precio = 'span.vtex-product-price-1-x-sellingPrice>span.vtex-product-price-1-x-sellingPriceValue--summary';
        selector_descripcion = '.vtex-product-summary-2-x-productNameContainer'
        selector_links = 'a.vtex-product-summary-2-x-clearLink'
        selector_carga = 'span.vtex-search-result-3-x-showingProductsCount.b'
        selector_cantidad = 'div.vtex-search-result-3-x-totalProducts--layout span'
        
        cantidad = await page.evaluate('''selector => {
        const elemento = document.querySelector(selector);
            if (elemento) {
                const textoCompleto = elemento.textContent || '';
                const numero = textoCompleto.split(' ')[0];
                return numero.trim();
            }
            return null;}''', selector_cantidad)

        await page.wait_for_selector('strong.construmartcl-custom-apps-0-x-triggerSelectedStore');
        await page.wait_for_selector('#gallery-layout-container')
        
        await funciones.scroll_infinito(page, selector_carga, cantidad)
        await page.wait_for_selector('#gallery-layout-container')

        links = await funciones.extraer_lista_links(page, selector_links);
        precios = await funciones.extraer_lista_elementos_texto(page, selector_precio)
        descripciones = await funciones.extraer_lista_elementos_texto(page, selector_descripcion)

        max_length = max(len(links), len(precios), len(descripciones))
        precios.extend(['$0'] * (max_length - len(precios)))

        precios_limpio = [funciones.limpiar_formato_moneda(precio) for precio in precios]
        descripcion_sin_tildes = [funciones.quitar_tildes(descripcion) for descripcion in descripciones]


        df = pd.DataFrame({
            'Link': links,
            'Tienda': 'Construmart',
            'Precio': precios_limpio,
            'Descripcion': descripcion_sin_tildes,
            'Region': funciones.quitar_tildes(region),
            'Comuna': funciones.quitar_tildes(comuna),
            'Fecha': fecha.strftime("%Y-%m-%d")
        })

        await browser.close()

        return df
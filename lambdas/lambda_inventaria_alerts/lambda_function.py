# lambda_function.py

from bsale_utils import fetch_variant_cost, returns_fetch, variants_fetch, products_fetch, consumptions_fetch, stocks_fetch, shippings_fetch
from inventaria_database import upload_metric_2, insert_product_to_db, upload_returns_inventaria_sheet, inventaria_upload_variants, get_products, upload_stocks, get_inventaria_metrics, get_inventaria_sheet_data, upload_shippings_inventaria_sheet, get_inventaria_stocks, upload_consumptions_inventaria_sheet
from inventaria_bsale_alerts import returns_qty_alert, hand_on_alert, zero_stock_alert, critical_stock_alert, low_rotation_alert, fix_stock
from utils.bsale_email import send_alert_email
from datetime import datetime, timedelta
import pytz
import time

def lambda_handler(event=None, context=None):
    """
    Función principal de Lambda para gestionar el flujo de alertas e inventarios.
    """
    start_time = time.time()
    chile_tz = pytz.timezone('America/Santiago')
    today = datetime.now(chile_tz).date()
    date_obj = (today - timedelta(days=10))

    # products = products_fetch()
    # product_map = {product['id']: product for product in products}

#     # Actualizamos tabla Product
#     variants = variants_fetch()
#     inventaria_upload_variants(variants, product_map=product_map)
    
#    # Actualizamos costo de producto
#     products = get_products()
#     for product in products:
#         source_id = product['source_id']
#         variant_cost = fetch_variant_cost(source_id) 

#         product['cost'] = variant_cost
#         insert_product_to_db(product)

    stocks = get_inventaria_stocks()
    shippings = get_inventaria_sheet_data("1900-01-01", datetime.now().strftime('%Y-%m-%d'), "mov_shipping")
    consumptions = get_inventaria_sheet_data("1900-01-01", datetime.now().strftime('%Y-%m-%d'), "mov_consumption")
    returns = get_inventaria_sheet_data("1900-01-01", datetime.now().strftime('%Y-%m-%d'), "mov_return")

    products = get_products()
    product_map = {product['id']: product for product in products}

    zero_stock_alert(stocks, products)
    critical_stock_alert(stocks, products)
    hand_on_alert(stocks, products)
    low_rotation_alert(shippings, stocks, products)
    fix_stock(products, consumptions)
    returns_qty_alert(products, date_obj, returns)

    # Enviamos el resumen de alertas
    today_metrics = get_inventaria_metrics(today, 1)
    
    total_stock_quantity = 0
    total_metric_price = 0

    # Calcular total de stock
    for stock in stocks:
        product_id = stock['product_id']
        product_cost = product_map.get(product_id)['cost']
        if stock['date'] == today:
            total_stock_quantity += stock['stock_quantity'] * product_cost

    # Calcular total de price de las métricas
    for metric in today_metrics:
        if metric['date'] == today.strftime('%Y-%m-%d'): 
            total_metric_price += metric['price']

    metric_2 = {
        'date' : today,
        'createdAt' : today,
        'updatedAt' : today,
        'deviation' : total_metric_price,
        'net' : total_stock_quantity,
        'companyId' : 4,
    }
    upload_metric_2(metric_2=metric_2)
    
    if today_metrics:
        response = send_alert_email("🚨 Inventaria - Estado de alertas diarias", today_metrics)
    else:
        print("No hay alertas para enviar hoy.")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tiempo total de ejecución: {elapsed_time:.2f} segundos.")

    return {
        'statusCode': 200,
        'body': f"Lambda ejecutada en {elapsed_time:.2f} segundos."
    }

if __name__ == "__main__":
    result = lambda_handler(None, None)
    print(result)

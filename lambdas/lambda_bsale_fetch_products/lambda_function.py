# lambda_function.py

from bsale_utils import fetch_variant_cost, returns_fetch, variants_fetch, products_fetch, consumptions_fetch, stocks_fetch, shippings_fetch
from inventaria_database import upload_metric_2, insert_product_to_db, upload_returns_inventaria_sheet, inventaria_upload_variants, get_products, upload_stocks, get_inventaria_metrics, get_inventaria_sheet_data, upload_shippings_inventaria_sheet, get_inventaria_stocks, upload_consumptions_inventaria_sheet
from inventaria_bsale_alerts import returns_qty_alert, hand_on_alert, zero_stock_alert, critical_stock_alert, low_rotation_alert, fix_stock
from utils.bsale_email import send_alert_email
from datetime import datetime, timedelta
import pytz
import time

def lambda_handler(event=None, context=None):
    start_time = time.time()
    chile_tz = pytz.timezone('America/Santiago')
    today = datetime.now(chile_tz).date()
    date_obj = (today - timedelta(days=10))

    products = products_fetch()
    product_map = {product['id']: product for product in products}

    # Actualizamos tabla Product
    variants = variants_fetch()
    inventaria_upload_variants(variants, product_map=product_map)
    
   # Actualizamos costo de producto
    products = get_products()
    for product in products:
        source_id = product['source_id']
        variant_cost = fetch_variant_cost(source_id) 

        product['cost'] = variant_cost
        insert_product_to_db(product)

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

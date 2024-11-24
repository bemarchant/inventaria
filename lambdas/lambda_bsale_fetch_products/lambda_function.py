import sys
import os

is_local = os.environ.get("AWS_EXECUTION_ENV") is None

if is_local:
    layer_path = os.path.join(
        os.path.dirname(__file__), '..', '..', 'layers', 'inventaria_layer', 'python'
    )
    if layer_path not in sys.path:
        sys.path.append(layer_path)

from bsale_utils import fetch_variant_cost, variants_fetch, products_fetch
from inventaria_database import upload_metric_2, insert_product_to_db, upload_returns_inventaria_sheet, inventaria_upload_variants, get_products, upload_stocks, get_inventaria_metrics, get_inventaria_sheet_data, upload_shippings_inventaria_sheet, get_inventaria_stocks, upload_consumptions_inventaria_sheet
import time

def lambda_handler(event=None, context=None):
    start_time = time.time()

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

import sys
import os

is_local = os.environ.get("AWS_EXECUTION_ENV") is None

if is_local:
    layer_path = os.path.join(
        os.path.dirname(__file__), '..', '..', 'layers', 'inventaria_layer', 'python'
    )
    if layer_path not in sys.path:
        sys.path.append(layer_path)

from inventaria_database import upload_metric_2, get_products, get_inventaria_metrics, get_inventaria_sheet_data, get_inventaria_stocks
from inventaria_bsale_alerts import returns_qty_alert, hand_on_alert, zero_stock_alert, critical_stock_alert, low_rotation_alert, fix_stock
from utils.bsale_email import send_alert_email
from datetime import datetime, timedelta
import pytz
import time

def lambda_handler(event=None, context=None):

    start_time = time.time()
    chile_tz = pytz.timezone('America/Santiago')
    current_datetime = datetime.now(chile_tz)

    stocks = get_inventaria_stocks()
    shippings = get_inventaria_sheet_data("1900-01-01", current_datetime.strftime('%Y-%m-%d'), "mov_shipping")
    consumptions = get_inventaria_sheet_data("1900-01-01", current_datetime.strftime('%Y-%m-%d'), "mov_consumption")
    returns = get_inventaria_sheet_data("1900-01-01", current_datetime.strftime('%Y-%m-%d'), "mov_return")

    products = get_products()
    product_map = {product['id']: product for product in products}

    zero_stock_alert(stocks, products)
    critical_stock_alert(current_datetime, stocks, products)
    hand_on_alert(current_datetime, stocks, products)
    low_rotation_alert(current_datetime, shippings, stocks, products)
    fix_stock(current_datetime, products, consumptions)
    returns_qty_alert(current_datetime, products, returns)

    # Enviamos el resumen de alertas
    today_metrics = get_inventaria_metrics(current_datetime, 1)
    
    total_stock_quantity = 0
    total_metric_price = 0

    # Calcular total de stock

    for stock in stocks:
        product_id = stock['product_id']
        product_cost = product_map.get(product_id)['cost']
        if stock['date'] == current_datetime.date():
            total_stock_quantity += stock['stock_quantity'] * product_cost

    # Calcular total de price de las métricas
    for metric in today_metrics:
        if metric['date'] == current_datetime.strftime('%Y-%m-%d'): 
            total_metric_price += metric['price']

    metric_2 = {
        'date' : current_datetime.date(),
        'createdAt' : current_datetime,
        'updatedAt' : current_datetime,
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

# lambda_function.py

from bsale_utils import returns_fetch, consumptions_fetch, stocks_fetch, shippings_fetch
from inventaria_database import upload_returns_inventaria_sheet, upload_stocks, upload_shippings_inventaria_sheet, upload_consumptions_inventaria_sheet
from datetime import datetime, timedelta
import pytz
import time

def lambda_handler(event=None, context=None):

    start_time = time.time()
    chile_tz = pytz.timezone('America/Santiago')
    today = datetime.now(chile_tz).date()
    date_str = (today - timedelta(days=10)).strftime('%Y-%m-%d')

    #Actualizamos el stock de los productos
    stocks = stocks_fetch()
    upload_stocks(stocks)

    shippings = shippings_fetch(date_str)
    upload_shippings_inventaria_sheet(shippings=shippings)
    consumptions = consumptions_fetch(date_str)
    upload_consumptions_inventaria_sheet(consumptions=consumptions) 
    returns = returns_fetch(date_str)
    upload_returns_inventaria_sheet(returns)

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

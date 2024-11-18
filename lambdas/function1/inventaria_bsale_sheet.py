import requests
from datetime import datetime, timedelta
from api_bsale import shippings_fetch, stock_receptions_fetch, consumptions_fetch, get_variants
from inventaria_database import upload_shippings_inventaria_sheet, inventaria_upload_variants
from utils import utils

# Parámetros de la API de Bsale
base_url = "https://api.bsale.io/v1"
access_token = "10fe11c752e82a0159f61cbf40791b96b287fbf9"
spreadsheet_id = "1N3_Gx9SqhMJZDo0PD5OEdaPaMIyMWuQqRL8ragExZxc"

query_date = '2024-11-03'

# receptions = stock_receptions_fetch(query_date)
# print(f"receptions : {receptions}")

shippings = shippings_fetch(query_date)
print(f"shippings : {shippings}")
upload_shippings_inventaria_sheet(shippings)

# CODIGO PARA VER ENTRE RANGOS DE FECHAS LOS CONSUMOS
# start_date = '2022-10-01'
# end_date = '2024-10-31'

# start = datetime.strptime(start_date, "%Y-%m-%d")
# end = datetime.strptime(end_date, "%Y-%m-%d")
# delta = timedelta(days=1)

# date_list = []
# while start <= end:
#     date_list.append(start.strftime("%Y-%m-%d"))
#     start += delta

# for date in date_list[::-1]:
#     print(f"date : {date}")
#     consumptions = consumptions_fetch(base_url, access_token, date)
#     print(consumptions)

# CODIGO PARA ACTUALIZAR VARIANTES EN PRODUCT TABLE
# variants = get_variants()
# variants = variants[::-1]
# inventaria_upload_variants(variants)


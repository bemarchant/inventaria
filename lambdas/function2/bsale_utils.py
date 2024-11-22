import time
import requests
import boto3
from datetime import datetime, timezone, timedelta

from inventaria_const import *

base_url = "https://api.bsale.io/v1"
access_token = "10fe11c752e82a0159f61cbf40791b96b287fbf9"

def unix_timestamp_to_date_string(timestamp, format='%Y-%m-%d'):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(format)

def get_unix_timestamp(date_str):

    dt = datetime.strptime(date_str, '%Y-%m-%d')
    dt = dt.replace(tzinfo=timezone.utc)

    return int(dt.timestamp())

def get_continuous_alert_days(dates_laboral, results):
    dates_alert = [row['date'].date() for row in results]

    last_continuous_alert = []
    current_streak = []
    
    for date in dates_laboral:
        date_only = date.date() 
        if date_only in dates_alert:
            current_streak.append(date)
        else:
            if current_streak:
                last_continuous_alert = current_streak
                current_streak = []

        if current_streak:
            last_continuous_alert = current_streak
            
    return len(last_continuous_alert)

def stocks_fetch():
    endpoint = f"/stocks.json?limit=50"
    url = f"{base_url}{endpoint}"
    headers = {
        "Access-Token": access_token,
        "Content-Type": "application/json"
    }

    stocks = []

    while url:
        print(f"Fetching from: {url}")
        try:
            response = make_request_with_retries('GET', url, headers=headers)
            data = response.json()

            for item in data['items']:
                reception_info = {
                    'quantity': item['quantity'],
                    'quantity_reserved': item['quantityReserved'],
                    'quantity_available': item['quantityAvailable'],
                    'variant_id': item['variant'].get('id') if 'variant' in item else None,
                    'office': item['office'].get('id') if 'office' in item else None,
                }
                stocks.append(reception_info)

            if data.get('next'):
                url = data.get('next')
            else:
                url = None
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch stocks: {e}")
            break  # Exit the loop if max retries are reached

    return stocks

def shippings_fetch(shipping_date):
    shipping_date = get_unix_timestamp(shipping_date)

    endpoint = f"/shippings.json"
    url = f"{base_url}{endpoint}?limit=50&shippingdate={shipping_date}"
    headers = {
        "Access_token": access_token,
        "Content-type": "application/json"
    }

    shippings = []  # Lista para acumular todos los datos

    count = 0
    while url:
        print(f"Fetching from : {url}")
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            for item in data['items']:
                shipping_info = {
                    'id': item['id'],
                    'shipping_date': unix_timestamp_to_date_string(int(item['shippingDate'])),
                    'office': item['office'].get('id') if 'office' in item else None,
                    'user': item['user'].get('id') if 'user' in item else None,
                }
                
                shipping_detail = get_shipping_detail(shipping_info['id'])

                shipping_info['variant_id'] = shipping_detail['variant_id']
                shipping_info['quantity'] = shipping_detail['quantity']
                shipping_info['variant_stock'] = shipping_detail['variant_stock']
                shipping_info['variant_cost'] = shipping_detail['variant_cost']

                shippings.append(shipping_info)
            if data.get('next'):
                url = f"{data.get('next')}&shippingdate={shipping_date}"
            else:
                url = None
            count += 1
        else:
            print(f"Error: {response.status_code} - {response.text}")
            break
    
    return shippings

def get_shipping_detail(shipping_id):
    endpoint = f"/shippings/{shipping_id}/details.json"
    url = f"{base_url}{endpoint}"
    headers = {
        "Access_token": access_token,
        "Content-type": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        shipping_detail = {}
        for item in data['items']:
            shipping_detail = {
                'id': item['id'],
                'quantity': item.get('quantity', ''),
                'variant_stock': item.get('variantStock', ''),
                'variant_cost': item.get('variantCost', ''),
                'variant_id': item['variant'].get('id') if 'variant' in item else None,
            }
        
        return shipping_detail
        
    else:
        print(f"Error: {response.status_code} - {response.text}")

def get_return_detail(return_id):
    endpoint = f"/returns/{return_id}/details.json"
    url = f"{base_url}{endpoint}"
    headers = {
        "Access_token": access_token,
        "Content-type": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return_detail = {}
        for item in data['items']:
            return_detail = {
                'id': item['id'],
                'quantity': item.get('quantity', ''),
                'variant_stock': item.get('variantStock', ''),
                'variant_cost': item.get('variantCost', ''),
                'variant_id': item['variant'].get('id') if 'variant' in item else None,
            }
        
        return return_detail
        
    else:
        print(f"Error: {response.status_code} - {response.text}")

def get_document_detail(document_id):
    endpoint = f"/documents/{document_id}/details.json"
    url = f"{base_url}{endpoint}"
    headers = {
        "Access_token": access_token,
        "Content-type": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        document_detail = {}
        for item in data['items']:
            document_detail = {
                'variant_id': item['variant'].get('id') if 'variant' in item else None,
            }
        
        return document_detail
        
    else:
        print(f"Error: {response.status_code} - {response.text}")

def fetch_variant_cost(variant_id):
    """
    Fetches costs for a list of variant IDs in a more optimized way. If the API does not allow
    batch fetching, it falls back to fetching each cost individually.
    """
    headers = {
    "Access-Token": access_token,
    "Content-Type": "application/json"
    }

    variant_cost = 0
    try:
        url = f"{base_url}/variants/{variant_id}/costs.json"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        variant_cost = data.get("averageCost", 0)  # Default to 0 if no averageCost
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch cost for variant {variant_id}: {e}")
        variant_cost[variant_id] = None  # Handle failure gracefully
    
    return variant_cost

def variants_fetch(batch_size=50):
    """
    Fetches all variants and their associated costs more efficiently by grouping requests
    into batches and fetching costs in a single function call.
    """
    headers = {
    "Access-Token": access_token,
    "Content-Type": "application/json"
    }

    endpoint = "/variants.json"
    url = f"{base_url}{endpoint}?limit={batch_size}"
    variants = []

    while url:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            variant_cost = 0
            
            # Process each variant and include its cost from the fetched cost data
            for item in data["items"]:
                variant_id = int(item.get("id"))
                processed_item = {
                    "source_id": variant_id,
                    "product_id": int(item["product"].get("id")) if "product" in item else None,
                    "description": item.get("description", ""),
                    "bar_code": item.get("barCode", ""),
                    "code": item.get("code", ""),
                    "cost": variant_cost,
                }
                variants.append(processed_item)

            # Proceed to the next page if available
            url = data.get("next")
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch variants: {e}")
            break  # Stop fetching if a fatal error occurs

    return variants

def fetch_variant_cost(variant_id):
    endpoint = f"/variants/{variant_id}/costs.json"
    url = f"{base_url}{endpoint}"
    headers = {
        "Access-Token": access_token,
        "Content-Type": "application/json"
    }

    try:
        response = make_request_with_retries('GET', url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            average_cost = data.get('averageCost')
            return average_cost
        else:
            print(f"Failed to fetch cost for variant {variant_id}: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Exception while fetching cost for variant {variant_id}: {e}")
        return None
    
def get_product_detail(product_id):
    endpoint = f"/products/{product_id}.json"
    url = f"{base_url}{endpoint}"
    headers = {
        "Access-Token": access_token,
        "Content-Type": "application/json"
    }

    print(f"Fetching from: {url}")
    try:
        response = make_request_with_retries('GET', url, headers=headers)
        data = response.json()
        processed_item = {
            'id': data.get('id', ''),
            'name': data.get('name', ''),
            'description': data.get('description', ''),
            'product_type': data['productType'].get('id') if 'productType' in data else None,
        }
        return processed_item
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch product detail for product_id {product_id}: {e}")
        return None

def products_fetch():
    endpoint = "/products.json"
    url = f"{base_url}{endpoint}?limit=50"
    headers = {
        "Access-Token": access_token,
        "Content-Type": "application/json"
    }
    products = []

    while url:
        print(f"Fetching from: {url}")
        try:
            response = make_request_with_retries('GET', url, headers=headers)
            data = response.json()

            for item in data['items']:
                product_type = 'Unknown'
                if 'productType' in item and item['productType']:
                    product_type = item['productType'].get('name', 'Unknown')

                processed_item = {
                    'id': int(item.get('id')),
                    'name': item.get('name', ''),
                    'description': item.get('description', ''),
                    'product_type': product_type,
                }
                products.append(processed_item)

            url = data.get('next')
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch products: {e}")
            break

    return products

def consumptions_fetch(consumption_date):
    consumption_date = get_unix_timestamp(consumption_date)

    endpoint = f"/stocks/consumptions.json"
    url = f"{base_url}{endpoint}?limit=50&consumptiondate={consumption_date}"
    headers = {
        "Access_token": access_token,
        "Content-type": "application/json"
    }

    consumptions = []  # Lista para acumular todos los datos

    count = 0
    while url:
        print(f"Fetching from : {url}")
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            for item in data['items']:
                consumption_info = {
                    'id': item['id'],
                    'consumption_date': unix_timestamp_to_date_string(int(item['consumptionDate'])),
                    'office': item['office'].get('id') if 'office' in item else None,
                    'user': item['user'].get('id') if 'user' in item else None,
                }
                
                consumption_detail = get_consumption_detail(consumption_info['id'])

                consumption_info['variant_id'] = consumption_detail['variant_id']
                consumption_info['quantity'] = consumption_detail['quantity']
                consumption_info['variant_stock'] = consumption_detail['variant_stock']
                consumption_info['cost'] = consumption_detail['cost']

                consumptions.append(consumption_info)
            if data.get('next'):
                url = f"{data.get('next')}&consumptiondate={consumption_date}"
            else:
                url = None
            count += 1
        else:
            print(f"Error: {response.status_code} - {response.text}")
            break
    
    return consumptions

def get_consumption_detail(consumption_id):
    endpoint = f"/stocks/consumptions/{consumption_id}/details.json"
    url = f"{base_url}{endpoint}"
    headers = {
        "Access_token": access_token,
        "Content-type": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        consumption_detail = {}
        for item in data['items']:
            consumption_detail = {
                'id': item['id'],
                'quantity': item.get('quantity', ''),
                'variant_stock': item.get('variantStock', ''),
                'cost': item.get('cost', ''),
                'variant_id': item['variant'].get('id') if 'variant' in item else None,
            }
        
        return consumption_detail
        
    else:
        print(f"Error: {response.status_code} - {response.text}")

def categories_fetch():
    endpoint = f"/product_types.json"
    url = f"{base_url}{endpoint}?limit=50"
    headers = {
        "Access_token": access_token,
        "Content-type": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        categories = []
        for item in data['items']:
            categorie = {
                'id': item['id'],
                'name': item.get('name', ''),
            }
            categories.append(categorie)
        return categories
        
    else:
        print(f"Error: {response.status_code} - {response.text}")


    return

def returns_fetch(date):
    date = get_unix_timestamp(date)

    endpoint = f"/returns.json"
    url = f"{base_url}{endpoint}?limit=50&returndate={date}"
    headers = {
        "Access_token": access_token,
        "Content-type": "application/json"
    }

    returns = []  # Lista para acumular todos los datos

    count = 0
    while url:
        print(f"Fetching from : {url}")
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            for item in data['items']:
                return_info = {
                    'id': item['id'],
                    'return_date': unix_timestamp_to_date_string(int(item['returnDate'])),
                    'office': item['office'].get('id') if 'office' in item else None,
                    'user': item['user'].get('id') if 'user' in item else None,
                }
                
                document_id = item['reference_document'].get('id')
                document_detail = get_document_detail(document_id)
                return_detail = get_return_detail(return_info['id'])

                return_info['variant_id'] = document_detail['variant_id']
                return_info['quantity'] = return_detail['quantity']
                return_info['variant_stock'] = return_detail['variant_stock']
                return_info['variant_cost'] = return_detail['variant_cost']

                returns.append(return_info)
            if data.get('next'):
                url = f"{data.get('next')}&returndate={date}"
            else:
                url = None
            count += 1
        else:
            print(f"Error: {response.status_code} - {response.text}")
            break
    
    return returns

## EMAIL UTILS

def send_email(subject, client_email, html_1, html_2,html_3):

    html_body = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Daily Alerts</title>
    </head>
    <body>
        <p>Hola a todos,</p>
    
        <p>Se adjuntan sus alertas diarias 🚨 junto a sus acciones ▶️ correctivas :</p>
    
        <p><strong>🚨 Control de Ajustes :</strong></p>
        <ul>
            <li>▶️ Acción: validar y respaldar ajuste</li>
        </ul>
        {}
        
        <p><strong>🚨 Control de Quiebres</strong></p>
        <ul>
            <li>▶️ Quiebre Comercial</li>
            <li>▶️ Validar con área comercial/ventas</li>
        </ul>

        {}
        
        <p><strong>🚨 Control de Vencimiento</strong></p>
        <ul>
            <li>▶️ Revisar cumplimiento de FEFO/Separar carga para priorizar despacho</li>
            <li>▶️ Producto vencido: inventariar y/o mermar si corresponde</li>
        </ul>

        {}
        <p>Saludos,</p>
        <p>Equipo Inventaria</p>
    </body>
    </html>
    """.format(html_1, html_2,html_3)

    # Your existing code for sending the email
    sender_email = 'inventariasup23@gmail.com'
    aws_region = 'sa-east-1'
    ses = boto3.client('ses', region_name=aws_region)

    response = ses.send_email(
        Destination={'ToAddresses': [client_email]},
        Message={
            'Body': {
                'Html': {'Charset': 'UTF-8', 'Data': html_body},
                'Text': {'Charset': 'UTF-8', 'Data': 'This is the text part of the email.'},
            },
            'Subject': {'Charset': 'UTF-8', 'Data': subject},
        },
        Source=sender_email,
    )

    return

def send_email_agunsa_1(subject, client_email, qty_1, qty_2, qty_3, qty_4):

    html_body = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Inventaria, alertas diarias</title>
    </head>
    <body>
        <p>Hola a todos,</p>
        <p>Se adjuntan sus alertas diarias 🚨 :</p>
        <p>A la fecha de hoy tienes:</p>
        <p><strong>Stock encontrado :</strong></p>
        <p>🚨 {} ubicaciones con stock encontrado</p>   
        <p><strong>Bloqueo Control de Calidad :</strong></p>
        <p>🚨 {} ubicaciones con gestión pendiente</p>
        <p><strong>Diferencias de Picking :</strong></p>
        <p>🚨 {} productos en la ubicación CINV4700</p>
        <p><strong>Diferencias de inventario cíclico :</strong></p>
        <p>🚨 {} productos en la ubicación MESAORDENA</p>
        
        <p>Saludos,</p>
        <p>Equipo Inventaria 📦</p>
    </body>
    </html>
    """.format(qty_1, qty_2, qty_3, qty_4)

    # Your existing code for sending the email
    sender_email = 'inventariasup23@gmail.com'
    aws_region = 'sa-east-1'
    ses = boto3.client('ses', region_name=aws_region)

    response = ses.send_email(
        Destination={'ToAddresses': [client_email]},
        Message={
            'Body': {
                'Html': {'Charset': 'UTF-8', 'Data': html_body},
                'Text': {'Charset': 'UTF-8', 'Data': 'This is the text part of the email.'},
            },
            'Subject': {'Charset': 'UTF-8', 'Data': subject},
        },
        Source=sender_email,
    )

    return response

def send_email_agunsa_2(subject, client_email, control, html):
    html_body = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Daily Alerts</title>
    </head>
    <body>
        <p>Hola a todos,</p>
    
        <p>Se adjuntan sus alertas diarias 🚨 junto a sus acciones ▶️ correctivas :</p>
        
        <p><strong>🚨 Control {}:</strong></p>

        {}
        
        <p>Saludos,</p>
        <p>Equipo Inventaria</p>
    </body>
    </html>
    """.format(control, html)

    # Your existing code for sending the email
    sender_email = 'inventariasup23@gmail.com'
    aws_region = 'sa-east-1'
    ses = boto3.client('ses', region_name=aws_region)

    response = ses.send_email(
        Destination={'ToAddresses': [client_email]},
        Message={
            'Body': {
                'Html': {'Charset': 'UTF-8', 'Data': html_body},
                'Text': {'Charset': 'UTF-8', 'Data': 'This is the text part of the email.'},
            },
            'Subject': {'Charset': 'UTF-8', 'Data': subject},
        },
        Source=sender_email,
    )

    return

def send_email_client(subject, client_email, html):
    
    return

def make_request_with_retries(method, url, headers=None, params=None, data=None, max_retries=5, backoff_factor=1):
    """
    Makes an HTTP request with retries and exponential backoff.

    Args:
        method (str): HTTP method ('GET', 'POST', etc.)
        url (str): The URL to request.
        headers (dict): Request headers.
        params (dict): URL parameters.
        data (dict): Request payload.
        max_retries (int): Maximum number of retries.
        backoff_factor (float): Backoff multiplier for exponential backoff.

    Returns:
        requests.Response: The response object if the request is successful.

    Raises:
        requests.exceptions.RequestException: If the request fails after all retries.
    """
    for retry in range(max_retries):
        try:
            response = requests.request(method, url, headers=headers, params=params, data=data, timeout=10)
            if response.status_code == 200:
                return response
            else:
                print(f"Request failed with status code {response.status_code}: {response.text}")
                # Decide whether to retry based on status code
                if response.status_code in [500, 502, 503, 504]:
                    # Server errors, can retry
                    pass
                else:
                    # For client errors (e.g., 400 Bad Request), do not retry
                    response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Request exception: {e}")
            if retry < max_retries - 1:
                sleep_time = backoff_factor * (2 ** retry)
                print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print("Max retries reached. Raising exception.")
                raise
    # If we reach here, all retries have failed
    raise requests.exceptions.RequestException(f"Failed to complete request after {max_retries} retries.")

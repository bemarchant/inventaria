import logging
from datetime import datetime, timedelta
import pytz

from inventaria_database import (
    inventaria_upload_metrics,
    inventariaweb_upload_metrics,
    get_inventaria_sheet_data,
    get_continuous_alert_days,
    get_inventaria_metrics
)
from utils.utils import shipping_quantity_mean
from  bsale_utils import categories_fetch

# Configurar el logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parámetros de la API de Bsale
base_url = "https://api.bsale.io/v1"
access_token = "10fe11c752e82a0159f61cbf40791b96b287fbf9"
spreadsheet_id = "1N3_Gx9SqhMJZDo0PD5OEdaPaMIyMWuQqRL8ragExZxc"

def zero_stock_alert(stocks, products):
    """
    Genera y sube métricas de productos con stock cero.
    """

    # Filtrar stocks con cantidad cero
    zero_stock_stocks = [stock for stock in stocks if stock.get('stock_quantity', 0) == 0]
    logger.info(f"Total de productos con stock cero: {len(zero_stock_stocks)}")

    if not zero_stock_stocks:
        logger.info("No hay productos con stock cero para procesar.")
        return

    # Obtener todos los product_ids con stock cero
    logger.info(f"Total de productos obtenidos para métricas: {len(products)}")

    # Crear un mapeo de product_id a producto
    product_map = {product['id']: product for product in products}

    # Preparar el mapeo de categorías para convertir `category` a `name`
    categories = categories_fetch()
    category_map = {category['id']: category['name'] for category in categories}

    # Preparar las métricas
    metrics = []
    timezone = pytz.timezone('America/Santiago')
    current_datetime = datetime.now(timezone)

    alerts = get_inventaria_metrics(alert_type='stock_zero')
    for stock in zero_stock_stocks:
        try:
            product_id = int(stock.get('product_id'))
            product = product_map.get(product_id)

            if not product:
                logger.warning(f"No se encontró el producto con ID {product_id} para el stock cero.")
                continue

            alert_days = get_continuous_alert_days(alerts, product['code'], alert_type='stock_zero')
            price = product.get('cost', 0)

            category_value = product.get('category', 'Unknown')
            category_id = int(category_value) if str(category_value).isdigit() else 0
            lote_type_name = category_map.get(category_id, 'Sin Categoría')
            
            metric = {
                'metric_id': 'stock_zero',
                'product': product.get('code', 'Desconocido'),
                'inventaria_product' : product.get('id'),
                'name': f"{product.get('name', 'Desconocido')}",
                'lote_type': lote_type_name,
                'location': product.get('warehouse_id', ''),
                'located': 0,
                'ready': 0,
                'blocked': 0,
                'alert_days': alert_days,
                'alert_level': 1,                                     
                'date': current_datetime.date().strftime('%Y-%m-%d'),
                'price': 0,
                'created_at': current_datetime.isoformat(),
                'updated_at': current_datetime.isoformat(),
                'company_id' : 4,
                'store' : product.get('warehouse_id', 0),
            }

            metrics.append(metric)
            logger.debug(f"Métrica preparada para el producto ID {product_id}: {metric}")

        except ValueError as ve:
            logger.error(f"Error al convertir product_id o stock_quantity: {ve}")
        except Exception as e:
            logger.error(f"Error inesperado al procesar el stock: {e}")

    if metrics:
        logger.info(f"Total de métricas a subir: {len(metrics)}")
        try:
            inventaria_upload_metrics(metrics, batch_size=500)
            inventariaweb_upload_metrics(metrics, batch_size=500)

            logger.info("Métricas de stock cero subidas exitosamente.")
        except Exception as e:
            logger.error(f"Error al subir métricas de stock cero: {e}")
    else:
        logger.info("No se encontraron métricas de stock cero para subir.")

    return

def critical_stock_alert(stocks , products):
    """
    Genera y sube métricas de productos con stock crítico.
    """

    # Obtener todos los productos necesarios en una sola consulta
    logger.info(f"Total de productos obtenidos para métricas: {len(products)}")

    # Crear un mapeo de product_id a producto
    product_map = {product['id']: product for product in products}

    # Preparar el mapeo de categorías para convertir `category` a `name`
    categories = categories_fetch()
    category_map = {category['id']: category['name'] for category in categories}

    # Preparar las métricas
    metrics = []
    timezone = pytz.timezone('America/Santiago')
    current_datetime = datetime.now(timezone)

    # Obtener el promedio de cantidad de envíos para los últimos 30 días
    timezone = pytz.timezone('America/Santiago')
    today = datetime.now(timezone).date()
    yesterday = today - timedelta(days=1)
    start_date = today - timedelta(days=30)
    shipping_movements = get_inventaria_sheet_data(start_date.strftime('%Y-%m-%d'), today, movement_type="mov_shipping")
    
    alerts = get_inventaria_metrics(alert_type='stock_critical')

    categories = categories_fetch()

    for stock in stocks:
        try:
            product_id = int(stock.get('product_id'))
            stock_quantity = int(stock.get('stock_quantity'))
            product = product_map.get(product_id)

            if not product:
                logger.warning(f"No se encontró el producto con ID {product_id} para el stock crítico.")
                continue

            average_shipping_quantity = shipping_quantity_mean(shipping_movements, product_id)

            if stock_quantity <=  3 * average_shipping_quantity and stock_quantity != 0:
                alert_days = get_continuous_alert_days(alerts, product['code'], alert_type='stock_critical')
                price = product.get('cost', '')

                category_value = product.get('category', 'Unknown')
                category_id = int(category_value) if str(category_value).isdigit() else 0
                lote_type_name = category_map.get(category_id, 'Sin Categoría')
            
                metric = {
                    'metric_id': 'stock_critical',
                    'product': product.get('code', 'Desconocido'),
                    'inventaria_product' : product.get('id'),
                    'name': f"{product.get('name', 'Desconocido')}",
                    'lote_type': lote_type_name,
                    'location': product.get('warehouse_id', ''),
                    'located': stock_quantity,
                    'ready': 0,
                    'blocked': 0,
                    'alert_days': alert_days,
                    'alert_level': 1,
                    'date': current_datetime.date().strftime('%Y-%m-%d'),
                    'price': price,
                    'created_at': current_datetime.isoformat(),
                    'updated_at': current_datetime.isoformat(),
                    'company_id': 4,
                    'store' : product.get('warehouse_id', 0),
                }

                metrics.append(metric)
                logger.debug(f"Métrica preparada para el producto ID {product_id}: {metric}")

        except ValueError as ve:
            logger.error(f"Error al convertir product_id o stock_quantity: {ve}")
        except Exception as e:
            logger.error(f"Error inesperado al procesar el stock: {e}")

    if metrics:
        logger.info(f"Total de métricas a subir: {len(metrics)}")
        try:
            inventaria_upload_metrics(metrics, batch_size=500)
            inventariaweb_upload_metrics(metrics, batch_size=500)
            logger.info("Métricas de stock crítico subidas exitosamente.")
        except Exception as e:
            logger.error(f"Error al subir métricas de stock crítico: {e}")
    else:
        logger.info("No se encontraron métricas de stock crítico para subir.")

    return

def low_rotation_alert(shippings, stocks, products):
    """
    Genera y sube métricas de productos con baja rotación si el último movimiento
    de tipo 'mov_shipping' es mayor a 2 semanas.
    """
    try:
        logger.info(f"Total de productos obtenidos para verificar baja rotación: {len(products)}")

        if not products:
            logger.info("No hay productos para procesar baja rotación.")
            return

        # Preparar el mapeo de categorías para convertir `category` a `name`
        categories = categories_fetch()
        category_map = {category['id']: category['name'] for category in categories}

        # Crear un mapeo de `stocks` por `product_id` y `date`
        today_date = datetime.now(pytz.timezone('America/Santiago')).date()
        stock_map = {(stock['product_id'], stock['date']): stock['stock_quantity'] for stock in stocks}

        # Preparar las métricas
        metrics = []
        timezone = pytz.timezone('America/Santiago')
        current_datetime = datetime.now(timezone)
        two_weeks_ago = (current_datetime - timedelta(weeks=2)).date()
        alerts = get_inventaria_metrics(alert_type='low_rotation')

        for product in products:
            try:
                product_id = product['id']
                product_code =  product.get('code', 'Desconocido')
                product_name = product.get('name', 'Desconocido')
                last_shipping = get_last_shipping_date(shippings, product_id)

                stock_quantity = stock_map.get((product_id, today_date), 0)

                if (last_shipping is None or last_shipping < two_weeks_ago) and stock_quantity != 0:

                    alert_days = get_continuous_alert_days(alerts, product_name, alert_type='low_rotation')
                    category_value = product.get('category', 'Unknown')
                    category_id = int(category_value) if str(category_value).isdigit() else 0
                    lote_type_name = category_map.get(category_id, 'Sin Categoría')
                    
                    price =   float(product.get('cost', 0))

                    metric = {
                        'metric_id': 'low_rotation',
                        'product': product.get('code', 'Desconocido'),
                        'inventaria_product' : product.get('id'),
                        'name': f"{product.get('name', 'Desconocido')}",
                        'lote_type': lote_type_name,
                        'location': product.get('warehouse_id', ''),
                        'located': stock_quantity,
                        'ready': 0,
                        'blocked': 0,
                        'alert_days': alert_days,
                        'alert_level': 1,
                        'date': current_datetime.date().strftime('%Y-%m-%d'),
                        'price':  price,
                        'created_at': current_datetime.isoformat(),
                        'updated_at': current_datetime.isoformat(),
                        'company_id': 4,
                        'store' : product.get('warehouse_id', 0),
                    }
                    metrics.append(metric)

                    logger.debug(f"Métrica preparada para baja rotación, producto ID {product_id}-{product_code}-{product_name}: {metric}")

            except Exception as e:
                logger.error(f"Error al procesar el producto {product_id}-{product_code}-{product_name} para baja rotación: {e}")

        # Subir las métricas a la base de datos si hay alertas de baja rotación
        if metrics:
            try:
                inventaria_upload_metrics(metrics, batch_size=500)
                inventariaweb_upload_metrics(metrics, batch_size=500)
                logger.info("Métricas de baja rotación subidas exitosamente.")
                return metrics
            
            except Exception as e:
                logger.error(f"Error al subir métricas de baja rotación: {e}")
        else:
            logger.info("No se encontraron métricas de baja rotación para subir.")

    except Exception as e:
        logger.error(f"Error inesperado en la función de alerta de baja rotación: {e}")

def hand_on_alert(stocks, products):

    # Obtener todos los productos necesarios en una sola consulta
    logger.info(f"Total de productos obtenidos para métricas: {len(products)}")

    # Crear un mapeo de product_id a producto
    product_map = {product['id']: product for product in products}

    # Preparar el mapeo de categorías para convertir `category` a `name`
    categories = categories_fetch()
    category_map = {category['id']: category['name'] for category in categories}

    # Preparar las métricas
    metrics = []
    timezone = pytz.timezone('America/Santiago')
    current_datetime = datetime.now(timezone)

    # Obtener el promedio de cantidad de envíos para los últimos 30 días
    timezone = pytz.timezone('America/Santiago')
    today = datetime.now(timezone).date()
    start_date = today - timedelta(days=30)
    shipping_movements = get_inventaria_sheet_data(start_date.strftime('%Y-%m-%d'), today, movement_type="mov_shipping")
    
    alerts = get_inventaria_metrics(alert_type='hand_on')
    categories = categories_fetch()

    for stock in stocks:
        try:
            product_id = int(stock.get('product_id'))
            stock_quantity = int(stock.get('stock_quantity'))
            product = product_map.get(product_id)

            if not product:
                logger.warning(f"No se encontró el producto con ID {product_id} para el stock crítico.")
                continue

            average_shipping_quantity = shipping_quantity_mean(shipping_movements, product_id)
            is_hand_on = False
            if average_shipping_quantity > 0:
                is_hand_on = stock_quantity / average_shipping_quantity >= 30
            
            if  is_hand_on and stock_quantity != 0:
                alert_days = get_continuous_alert_days(alerts, product['code'], alert_type='hand_on')
                price = product.get('cost', '')

                category_value = product.get('category', 'Unknown')
                category_id = int(category_value) if str(category_value).isdigit() else 0
                lote_type_name = category_map.get(category_id, 'Sin Categoría')
            
                metric = {
                    'metric_id': 'hand_on',
                    'product': product.get('code', 'Desconocido'),
                    'inventaria_product' : product.get('id'),
                    'name': f"{product.get('name', 'Desconocido')}",
                    'lote_type': lote_type_name,
                    'location': product.get('warehouse_id', ''),
                    'located': stock_quantity,
                    'ready': 0,
                    'blocked': 0,
                    'alert_days': alert_days,
                    'alert_level': 1,
                    'date': current_datetime.date().strftime('%Y-%m-%d'),
                    'price': price * stock_quantity,
                    'created_at': current_datetime.isoformat(),
                    'updated_at': current_datetime.isoformat(),
                    'company_id': 4,
                    'store' : product.get('warehouse_id', 0),
                }

                metrics.append(metric)
                logger.debug(f"Métrica preparada para el producto ID {product_id}: {metric}")

        except ValueError as ve:
            logger.error(f"Error al convertir product_id o stock_quantity: {ve}")
        except Exception as e:
            logger.error(f"Error inesperado al procesar el stock: {e}")

    if metrics:
        logger.info(f"Total de métricas a subir: {len(metrics)}")
        try:
            inventaria_upload_metrics(metrics, batch_size=500)
            inventariaweb_upload_metrics(metrics, batch_size=500)
            logger.info("Métricas de stock crítico subidas exitosamente.")
        except Exception as e:
            logger.error(f"Error al subir métricas de stock crítico: {e}")
    else:
        logger.info("No se encontraron métricas de stock crítico para subir.")

    return

def fix_stock(products, consumptions):
    """
    Genera y sube métricas de productos con ajuste de stock si el movimiento es
    de tipo 'mov_consumption'
    """
    try:
        logger.info(f"Total de productos obtenidos para verificar ajuste de stock: {len(products)}")

        if not products:
            logger.info("No hay productos para procesar ajuste de stock.")
            return
        
        # Preparar el mapeo de categorías para convertir `category` a `name`
        categories = categories_fetch()
        category_map = {category['id']: category['name'] for category in categories}
        product_map = {product['source_id']: product for product in products}
        
        # Preparar las métricas
        metrics = []
        timezone = pytz.timezone('America/Santiago')
        current_datetime = datetime.now(timezone)
        yesterday_date = (current_datetime - timedelta(days=1)).date()

        alerts = get_inventaria_metrics(alert_type='fix_stock')

        for consumption in consumptions:
            consumption_date = consumption['date']
            if consumption_date != yesterday_date:
                continue
            try:
                variant_id = int(consumption.get('variant_id'))
                product = product_map.get(variant_id)
                quantity = consumption.get('quantity')
                cost =  int(consumption.get('cost'))

                category_value = product.get('category', 'Unknown')
                category_id = int(category_value) if str(category_value).isdigit() else 0
                lote_type_name = category_map.get(category_id, 'Sin Categoría')
    
                
                alert_days = get_continuous_alert_days(alerts, product['code'], alert_type='hand_on')
                
                metric = {
                    'metric_id': 'fix_stock',
                    'product': product.get('code', 'Desconocido'),
                    'inventaria_product' : product.get('id'),
                    'name': f"{product.get('name', 'Desconocido')}",
                    'lote_type': lote_type_name,
                    'location': product.get('warehouse_id', ''),
                    'located': quantity,
                    'ready': 0,
                    'blocked': 0,
                    'alert_days': alert_days,
                    'alert_level': 1,
                    'date': current_datetime.date().strftime('%Y-%m-%d'),
                    'price':  cost,
                    'created_at': current_datetime.isoformat(),
                    'updated_at': current_datetime.isoformat(),
                    'company_id': 4,
                    'store' : product.get('warehouse_id', 0),
                    }

                metrics.append(metric)

                logger.debug(f"Métrica preparada para ajuste de stock, producto ID {product.get('id')}: {metric}")

            except Exception as e:
                logger.error(f"Error al procesar el producto {product.get('id')} para ajuste de stock: {e}")

        # Subir las métricas a la base de datos si hay alertas de ajuste de stock
        if metrics:
            try:
                inventaria_upload_metrics(metrics, batch_size=500)
                inventariaweb_upload_metrics(metrics, batch_size=500)
                logger.info("Métricas de ajuste de stock subidas exitosamente.")
                return metrics
            
            except Exception as e:
                logger.error(f"Error al subir métricas de ajuste de stock: {e}")
        else:
            logger.info("No se encontraron métricas de ajuste de stock para subir.")

    except Exception as e:
        logger.error(f"Error inesperado en la función de alerta de ajuste de stock: {e}")

def returns_qty_alert(products, current_datetime, returns):
    """
    Genera y sube métricas de productos con cantidad de devoluciones si el movimiento es
    de tipo 'mov_consumption'
    """
    try:
        logger.info(f"Total de productos obtenidos para verificar cantidad de devoluciones: {len(products)}")

        if not products:
            logger.info("No hay productos para procesar cantidad de devoluciones.")
            return
        
        # Preparar el mapeo de categorías para convertir `category` a `name`
        categories = categories_fetch()
        category_map = {category['id']: category['name'] for category in categories}
        product_map = {product['source_id']: product for product in products}
        
        # Preparar las métricas
        metrics = []
        
        yesterday_date = (current_datetime - timedelta(days=1))

        alerts = get_inventaria_metrics(alert_type='return_qty')
        
        for item in returns:
            return_date = item['date']

            if return_date != yesterday_date:
                continue
            try:
                variant_id = int(item.get('product_id'))
                product = product_map.get(variant_id)
                quantity = item.get('quantity')
                cost =  int(item.get('cost'))

                category_value = product.get('category', 'Unknown')
                category_id = int(category_value) if str(category_value).isdigit() else 0
                lote_type_name = category_map.get(category_id, 'Sin Categoría')
    
                
                alert_days = get_continuous_alert_days(alerts, product['code'], alert_type='hand_on')
                
                metric = {
                    'metric_id': 'return_qty',
                    'product': product.get('code', 'Desconocido'),
                    'inventaria_product' : product.get('id'),
                    'name': f"{product.get('name', 'Desconocido')}",
                    'lote_type': lote_type_name,
                    'location': product.get('warehouse_id', ''),
                    'located': quantity,
                    'ready': 0,
                    'blocked': 0,
                    'alert_days': alert_days,
                    'alert_level': 1,
                    'date': current_datetime.date().strftime('%Y-%m-%d'),
                    'price':  cost,
                    'created_at': current_datetime.isoformat(),
                    'updated_at': current_datetime.isoformat(),
                    'company_id': 4,
                    'store' : product.get('warehouse_id', 0),
                    }

                metrics.append(metric)

                logger.debug(f"Métrica preparada para cantidad de devoluciones, producto ID {product.get('id')}: {metric}")

            except Exception as e:
                logger.error(f"Error al procesar el producto {product.get('id')} para cantidad de devoluciones: {e}")

        # Subir las métricas a la base de datos si hay alertas de cantidad de devoluciones
        if metrics:
            try:
                inventaria_upload_metrics(metrics, batch_size=500)
                inventariaweb_upload_metrics(metrics, batch_size=500)
                logger.info("Métricas de cantidad de devoluciones subidas exitosamente.")
                return metrics
            
            except Exception as e:
                logger.error(f"Error al subir métricas de cantidad de devoluciones: {e}")
        else:
            logger.info("No se encontraron métricas de cantidad de devoluciones para subir.")

    except Exception as e:
        logger.error(f"Error inesperado en la función de alerta de cantidad de devoluciones: {e}")

def get_last_shipping_date(shippings, product_id):
    """
    Obtiene la última fecha de movimiento de tipo 'mov_shipping' para un producto específico.

    Args:
        product_id (int): ID del producto a consultar.

    Returns:
        datetime.date: La última fecha de movimiento de tipo 'mov_shipping' o None si no hay movimientos.
    """
    try:
        # Obtener todos los movimientos de tipo 'mov_shipping' para el producto
        product_shippings = [shipping for shipping in shippings if shipping['product_id'] == product_id]

        if not product_shippings:
            return None  # No hay movimientos de envío para este producto

        # Encontrar la última fecha de envío
        last_shipping = max([shipping['date'] for shipping in product_shippings])

        return last_shipping

    except Exception as e:
        logger.error(f"Error al obtener la última fecha de envío para el producto {product_id}: {e}")
        return None

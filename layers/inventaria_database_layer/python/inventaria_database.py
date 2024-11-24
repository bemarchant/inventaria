from datetime import datetime, timedelta, timezone
from bsale_utils import get_product_detail
from inventaria_const import *

import psycopg2
import pytz
import logging
from psycopg2 import sql, extras
from psycopg2.extras import RealDictCursor

# Configurar el logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

base_url = "https://api.bsale.io/v1"
access_token = "10fe11c752e82a0159f61cbf40791b96b287fbf9"

# def get_continuous_alert_days(dates_laboral, results):
#     dates_alert = [row['date'].date() for row in results]

#     last_continuous_alert = []
#     current_streak = []
    
#     for date in dates_laboral:
#         date_only = date.date() 
#         if date_only in dates_alert:
#             current_streak.append(date)
#         else:
#             if current_streak:
#                 last_continuous_alert = current_streak
#                 current_streak = []

#         if current_streak:
#             last_continuous_alert = current_streak
            
#     return len(last_continuous_alert)

def get_db_connection():
    """
    Establece y retorna una conexión a la base de datos PostgreSQL.
    
    Returns:
        psycopg2.extensions.connection: Objeto de conexión a la base de datos.
    """
    try:
        conn = psycopg2.connect(
            host=INVENTARIA_POSTGRES_HOST,
            database=INVENTARIA_POSTGRES_DB,
            user=INVENTARIA_POSTGRES_USER,
            password=INVENTARIA_POSTGRES_PASSWORD,
            port=INVENTARIA_POSTGRES_PORT
        )
        return conn
    except psycopg2.Error as db_err:
        logger.error(f"Error al conectarse a la base de datos: {db_err}")
        return None

def insert_product_to_db(product):
    conn = psycopg2.connect(
            host=INVENTARIA_POSTGRES_HOST,
            database=INVENTARIA_POSTGRES_DB,
            user=INVENTARIA_POSTGRES_USER,
            password=INVENTARIA_POSTGRES_PASSWORD,
            port=INVENTARIA_POSTGRES_PORT
    )
    cursor = conn.cursor()

    # SQL query to insert a new product or update if it already exists
    insert_query = sql.SQL("""
        INSERT INTO "inventaria_app_product" (
            name, description, warehouse_id, source_id, bar_code, code, category, cost
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_id, warehouse_id) DO UPDATE SET
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            bar_code = EXCLUDED.bar_code,
            code = EXCLUDED.code,
            category = EXCLUDED.category,
            cost = EXCLUDED.cost;
    """)

    try:
        # Execute the query with cost included
        cursor.execute(insert_query, (
            product['name'],
            product['description'],
            product['warehouse_id'],
            product['source_id'],
            product['bar_code'],
            product['code'],
            product['category'],
            product['cost'] 
        ))
        conn.commit()
    except Exception as e:
        print(f"Error inserting variant {product['source_id']}: {e}")
    finally:
        cursor.close()
        conn.close()

def inventaria_upload_metrics(metrics, batch_size=1000):
    """
    Inserta o actualiza registros de métricas en la base de datos en lotes.
    
    Args:
        metrics (list): Lista de diccionarios con información de métricas.
        batch_size (int): Número de registros a insertar por lote.
    """
    if not metrics:
        logger.info("No hay métricas para procesar.")
        return

    # Preparar los datos para la inserción por lotes
    records = []
    for metric in metrics:
        # Validar que todas las claves necesarias estén presentes
        required_keys = ['metric_id', 'product', 'name', 'lote_type', 'location', 
                         'located', 'ready', 'blocked', 'alert_days', 'alert_level', 
                         'date', 'price', 'created_at', 'updated_at']
        if not all(key in metric for key in required_keys):
            logger.warning(f"Métrica incompleta y será omitida: {metric}")
            continue

        records.append((
            metric['metric_id'],
            metric['product'],
            metric['inventaria_product'],
            metric['name'],
            metric['lote_type'],
            metric['location'],
            metric['located'],
            metric['ready'],
            metric['blocked'],
            metric['alert_days'],
            metric['alert_level'],
            metric['date'],
            metric['price'],
            metric['created_at'],
            metric['updated_at']
        ))

    if not records:
        logger.info("No hay métricas válidas para insertar después de la validación.")
        return

    # Definir la consulta SQL con placeholders
    insert_query = sql.SQL("""
        INSERT INTO "inventaria_app_metric" (
            metric_id, product, inventaria_product_id, name, lote_type, location, located, ready, 
            blocked, alert_days, alert_level, date, price, created_at, updated_at
        ) VALUES %s
        ON CONFLICT (date, metric_id, product) DO UPDATE SET
            name = EXCLUDED.name,
            lote_type = EXCLUDED.lote_type,
            location = EXCLUDED.location,
            located = EXCLUDED.located,
            ready = EXCLUDED.ready,
            blocked = EXCLUDED.blocked,
            alert_days = EXCLUDED.alert_days,
            alert_level = EXCLUDED.alert_level,
            price = EXCLUDED.price,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at;
    """)

    conn = get_db_connection()
    if not conn:
        logger.error("No se pudo establecer conexión a la base de datos para subir métricas.")
        return

    try:
        with conn.cursor() as cursor:
            # Dividir los registros en lotes y ejecutar las inserciones
            total_records = len(records)
            logger.info(f"Total de métricas a insertar: {total_records}")

            for i in range(0, total_records, batch_size):
                batch = records[i:i + batch_size]
                logger.info(f"Insertando lote {i//batch_size + 1}: {len(batch)} métricas.")

                try:
                    extras.execute_values(
                        cursor, insert_query, batch, template=None, page_size=batch_size
                    )
                    conn.commit()
                    logger.info(f"Lote {i//batch_size + 1} insertado exitosamente.")
                except psycopg2.Error as db_err:
                    conn.rollback()
                    logger.error(f"Error al insertar el lote {i//batch_size + 1}: {db_err}")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado al subir métricas: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.info("Proceso de inserción de métricas finalizado.")

def inventariaweb_upload_metrics(metrics, batch_size=500):
    """
    Inserta o actualiza registros de métricas en la base de datos en lotes.
    
    Args:
        metrics (list): Lista de diccionarios con información de métricas.
        batch_size (int): Número de registros a insertar por lote.
    """
    if not metrics:
        logger.info("No hay métricas para procesar.")
        return

    # Preparar los datos para la inserción por lotes
    records = []
    for metric in metrics:
        # Validar que todas las claves necesarias estén presentes
        required_keys = ['metric_id', 'product', 'name', 'lote_type', 'location', 
                         'located', 'ready', 'blocked', 'alert_days', 'alert_level', 
                         'date', 'price', 'created_at', 'updated_at', 'store']
        if not all(key in metric for key in required_keys):
            logger.warning(f"Métrica incompleta y será omitida: {metric}")
            continue

        records.append((
            metric['metric_id'],
            metric['product'],
            metric['name'],
            metric['lote_type'],
            metric['location'],
            metric['located'],
            metric['ready'],
            metric['blocked'],
            metric['alert_days'],
            metric['alert_level'],
            metric['date'],
            metric['price'],
            metric['created_at'],
            metric['updated_at'],
            metric['company_id'],
            metric['store'],
        ))

    if not records:
        logger.info("No hay métricas válidas para insertar después de la validación.")
        return

    # Definir la consulta SQL con placeholders
    insert_query = sql.SQL("""
        INSERT INTO "metric1" (
            "metricId", "product", "name", "loteType", "location", "located", "ready", 
            "blocked", "alertDays", "alertLevel", "date", "price", "createdAt", "updatedAt", "companyId", "store"
        ) VALUES %s
        ON CONFLICT ("date", "metricId", "product") DO UPDATE SET
            "name" = EXCLUDED."name",
            "loteType" = EXCLUDED."loteType",
            "location" = EXCLUDED."location",
            "located" = EXCLUDED."located",
            "ready" = EXCLUDED."ready",
            "blocked" = EXCLUDED."blocked",
            "alertDays" = EXCLUDED."alertDays",
            "alertLevel" = EXCLUDED."alertLevel",
            "price" = EXCLUDED."price",
            "createdAt" = EXCLUDED."createdAt",
            "updatedAt" = EXCLUDED."updatedAt",
            "companyId" = EXCLUDED."companyId",
            "store" = EXCLUDED."store"
    """)

    conn = get_db_connection()
    if not conn:
        logger.error("No se pudo establecer conexión a la base de datos para subir métricas.")
        return

    try:
        with conn.cursor() as cursor:
            # Dividir los registros en lotes y ejecutar las inserciones
            total_records = len(records)
            logger.info(f"Total de métricas a insertar: {total_records}")

            for i in range(0, total_records, batch_size):
                batch = records[i:i + batch_size]
                logger.info(f"Insertando lote {i//batch_size + 1}: {len(batch)} métricas.")

                try:
                    extras.execute_values(
                        cursor, insert_query, batch, template=None, page_size=batch_size
                    )
                    conn.commit()
                    logger.info(f"Lote {i//batch_size + 1} insertado exitosamente.")
                except psycopg2.Error as db_err:
                    conn.rollback()
                    logger.error(f"Error al insertar el lote {i//batch_size + 1}: {db_err}")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado al subir métricas: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.info("Proceso de inserción de métricas finalizado.")
    return

def get_product_by_variant_id(variant_id):
    query = """
    SELECT *
    FROM inventaria_app_product
    WHERE source_id = %s;
    """

    try:
        # Conectar a la base de datos
        conn = psycopg2.connect(
            dbname=INVENTARIA_POSTGRES_DB,
            user=INVENTARIA_POSTGRES_USER,
            password=INVENTARIA_POSTGRES_PASSWORD,
            host=INVENTARIA_POSTGRES_HOST,
            port=INVENTARIA_POSTGRES_PORT
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query, (variant_id,))
        product = cur.fetchone() 

        cur.close()
        conn.close()
        return product

    except Exception as e:
        print(f"Error fetching product with variant_id {variant_id}: {e}")
        return None

def get_product_by_id(product_id):
    query = """
    SELECT *
    FROM inventaria_app_product
    WHERE id = %s;
    """

    try:
        # Conectar a la base de datos
        conn = psycopg2.connect(
            dbname=INVENTARIA_POSTGRES_DB,
            user=INVENTARIA_POSTGRES_USER,
            password=INVENTARIA_POSTGRES_PASSWORD,
            host=INVENTARIA_POSTGRES_HOST,
            port=INVENTARIA_POSTGRES_PORT
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query, (product_id,))
        product = cur.fetchone() 

        cur.close()
        conn.close()
        return product

    except Exception as e:
        print(f"Error fetching product with id {product_id}: {e}")
        return None

def get_products_by_ids(product_ids):
    """
    Obtiene múltiples productos de la base de datos basados en una lista de product_ids.
    
    Args:
        product_ids (list): Lista de IDs de productos a obtener.
    
    Returns:
        list: Lista de diccionarios que representan los productos.
    """
    if not product_ids:
        logger.info("No se proporcionaron product_ids para obtener productos.")
        return []

    query = """
    SELECT *
    FROM inventaria_app_product
    WHERE id IN %s;
    """

    conn = get_db_connection()
    if not conn:
        logger.error("No se pudo establecer conexión a la base de datos para obtener productos.")
        return []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (tuple(product_ids),))
            products = cursor.fetchall()
            logger.info(f"Se obtuvieron {len(products)} productos de la base de datos.")
            return products
    except psycopg2.Error as db_err:
        logger.error(f"Error al obtener productos: {db_err}")
        return []
    finally:
        conn.close()

def get_products():
    """
    Obtiene todos los productos de la base de datos.
    
    Returns:
        list: Lista de diccionarios que representan los productos.
    """

    query = """
    SELECT *
    FROM inventaria_app_product;
    """

    conn = get_db_connection()
    if not conn:
        logger.error("No se pudo establecer conexión a la base de datos para obtener productos.")
        return []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            products = cursor.fetchall()
            logger.info(f"Se obtuvieron {len(products)} productos de la base de datos.")
            return products
    except psycopg2.Error as db_err:
        logger.error(f"Error al obtener productos: {db_err}")
        return []
    finally:
        conn.close()

def load_products_by_variant_ids(variant_ids):
    """
    Carga productos desde la base de datos que corresponden a los variant_ids proporcionados.
    Retorna un diccionario mapeando variant_id a producto.
    """
    query = """
    SELECT *
    FROM inventaria_app_product
    WHERE source_id IN %s;
    """
    try:
        conn = psycopg2.connect(
            dbname=INVENTARIA_POSTGRES_DB,
            user=INVENTARIA_POSTGRES_USER,
            password=INVENTARIA_POSTGRES_PASSWORD,
            host=INVENTARIA_POSTGRES_HOST,
            port=INVENTARIA_POSTGRES_PORT
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, (tuple(variant_ids),))
        products = cursor.fetchall()
        cursor.close()
        conn.close()
        # Mapeo de variant_id a producto
        product_map = {str(product['source_id']): product for product in products}
        return product_map
    except Exception as e:
        print(f"Error al cargar productos: {e}")
        return {}

def upload_stocks(stocks, batch_size=1000):
    """
    Inserta o actualiza registros de stock en la base de datos en lotes.
    
    Args:
        stocks (list): Lista de diccionarios con información de stock.
        batch_size (int): Número de registros a insertar por lote.
    """
    # Extraer todos los variant_ids para pre-cargar productos
    variant_ids = [stock['variant_id'] for stock in stocks]
    product_map = load_products_by_variant_ids(variant_ids)

    # Preparar los datos para la inserción por lotes
    records = []
    missing_products = 0
    for stock in stocks:
        product = product_map.get(stock['variant_id'])
        if not product:
            logger.warning(f"Producto no encontrado para variant_id {stock['variant_id']}.")
            missing_products += 1
            continue

        # Convertir stock_quantity a entero si es necesario
        try:
            stock_quantity = int(float(stock['quantity']))
        except ValueError:
            logger.error(f"Cantidad de stock inválida: {stock['quantity']} para variant_id {stock['variant_id']}.")
            continue

        # Obtener la fecha actual en la zona horaria deseada
        chile_tz = pytz.timezone('America/Santiago')
        date = datetime.now(chile_tz).date()

        records.append((date, product['id'], stock_quantity))

    if missing_products > 0:
        logger.info(f"Se omitieron {missing_products} registros debido a productos no encontrados.")

    if not records:
        logger.info("No hay registros de stock para insertar.")
        return

    # Conectar a la base de datos una sola vez
    try:
        conn = psycopg2.connect(
            dbname=INVENTARIA_POSTGRES_DB,
            user=INVENTARIA_POSTGRES_USER,
            password=INVENTARIA_POSTGRES_PASSWORD,
            host=INVENTARIA_POSTGRES_HOST,
            port=INVENTARIA_POSTGRES_PORT
        )
        cursor = conn.cursor()

        # Definir la consulta SQL con placeholders
        insert_query = sql.SQL("""
            INSERT INTO "inventaria_app_stock" (
                date, product_id, stock_quantity
            ) VALUES %s
            ON CONFLICT (product_id, date) 
            DO UPDATE SET stock_quantity = EXCLUDED.stock_quantity;
        """)

        # Dividir los registros en lotes
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            extras.execute_values(
                cursor, insert_query, batch, template=None, page_size=batch_size
            )
            conn.commit()
            logger.info(f"Insertados/Actualizados {len(batch)} registros de stock.")

        cursor.close()
        conn.close()
        logger.info("Inserción de stocks completada exitosamente.")
    except Exception as e:
        logger.error(f"Error al insertar stocks: {e}")
        if conn:
            conn.rollback()
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_inventaria_stocks():
    """
    Obtiene una lista de diccionarios con los registros de stock de la fecha actual en UTC-3.
    
    Returns:
        list: Lista de diccionarios que representan los registros de stock.
    """
    timezone = pytz.timezone('America/Argentina/Buenos_Aires')
    today = datetime.now(timezone).date()

    query = """
        SELECT *
        FROM inventaria_app_stock
        WHERE date = %s;
    """

    conn = get_db_connection()
    if not conn:
        logger.error("No se pudo establecer conexión a la base de datos.")
        return []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (today,))
            records = cursor.fetchall()
            logger.info(f"Se encontraron {len(records)} registros de stock para la fecha {today}.")
            return records
    except psycopg2.Error as db_err:
        logger.error(f"Error al consultar la base de datos: {db_err}")
        return []
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")
        return []
    finally:
        conn.close()

def upload_shippings_inventaria_sheet(shippings):

    # Conectar a la base de datos
    conn = psycopg2.connect(
            dbname=INVENTARIA_POSTGRES_DB,
            user=INVENTARIA_POSTGRES_USER,
            password=INVENTARIA_POSTGRES_PASSWORD,
            host=INVENTARIA_POSTGRES_HOST,
            port=INVENTARIA_POSTGRES_PORT
    )
    cursor = conn.cursor()

    # Consulta SQL para insertar o actualizar un envío en la tabla InventariaSheet
    insert_query = sql.SQL("""
        INSERT INTO "inventaria_app_inventariasheet" (
            date, product_id, quantity, 
            cost, movement_type, movement_description, source_id, user_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, product_id, source_id) DO UPDATE SET
            quantity = EXCLUDED.quantity,
            cost = EXCLUDED.cost,
            movement_type = EXCLUDED.movement_type,
            movement_description = EXCLUDED.movement_description,
            user_id = EXCLUDED.user_id;
    """)


    for shipping in shippings:
        # Convertir la fecha de envío a un formato adecuado
        shipping_date = datetime.strptime(shipping['shipping_date'], '%Y-%m-%d')
        shipping_date = shipping_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        # Obtener detalles del producto basado en el variant_id
        product = get_product_by_variant_id(shipping['variant_id'])
        
        if not product:
            print(f"Producto no encontrado para variant_id {shipping['variant_id']}.")
            continue

        try:
            # Ejecutar la consulta de inserción para cada registro en shippings

            cursor.execute(insert_query, (
                shipping_date,
                product['id'],                      # ID del producto en la tabla relacionada
                shipping['quantity'],               # Cantidad enviada
                shipping['variant_cost'],           # Costo de la variante
                'mov_shipping',                     # Tipo de movimiento
                f"Shipping ID {shipping['id']}",    # Descripción del movimiento
                shipping['id'],                     # ID del movimiento
                int(shipping['user'])               # ID del usuario responsable del envío
            ))
            conn.commit()
            print(f"Envío registrado: {shipping['id']} para el producto {product['name']}.")
        except Exception as e:
            print(f"Error al insertar envío {shipping['id']}: {e}")
            conn.rollback()

    # Cerrar cursor y conexión
    cursor.close()
    conn.close()

    return

def upload_consumptions_inventaria_sheet(consumptions):
    print(consumptions)
    # Conectar a la base de datos
    conn = psycopg2.connect(
            dbname=INVENTARIA_POSTGRES_DB,
            user=INVENTARIA_POSTGRES_USER,
            password=INVENTARIA_POSTGRES_PASSWORD,
            host=INVENTARIA_POSTGRES_HOST,
            port=INVENTARIA_POSTGRES_PORT
    )
    cursor = conn.cursor()

    # Consulta SQL para insertar o actualizar un envío en la tabla InventariaSheet
    insert_query = sql.SQL("""
        INSERT INTO "inventaria_app_inventariasheet" (
            date, product_id, quantity, 
            cost, movement_type, movement_description, source_id, user_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, product_id, source_id) DO UPDATE SET
            quantity = EXCLUDED.quantity,
            cost = EXCLUDED.cost,
            movement_type = EXCLUDED.movement_type,
            movement_description = EXCLUDED.movement_description,
            user_id = EXCLUDED.user_id;
    """)

    for consumption in consumptions:
        print(f"upload_consumptions_inventaria_sheet")
        print(f"consumption : {consumption}")
        # Convertir la fecha de envío a un formato adecuado
        consumption_date = datetime.strptime(consumption['consumption_date'], '%Y-%m-%d')
        consumption_date = consumption_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        # Obtener detalles del producto basado en el variant_id
        product = get_product_by_variant_id(consumption['variant_id'])
        
        if not product:
            print(f"Producto no encontrado para variant_id {consumption['variant_id']}.")
            continue

        try:
            cost = int(consumption['cost']) if consumption['cost'] else 0
            cursor.execute(insert_query, (
                consumption_date,
                product['id'],                          # ID del producto en la tabla relacionada
                consumption['quantity'],                # Cantidad enviada
                cost,                                   # Costo
                'mov_consumption',                      # Tipo de movimiento
                f"consumption ID {consumption['id']}",  # Descripción del movimiento
                consumption['id'],                      # ID del movimiento
                int(consumption['user'])                # ID del usuario responsable del envío
            ))
            conn.commit()
            print(f"Envío registrado: {consumption['id']} para el producto {product['name']}.")
        except Exception as e:
            print(f"Error al insertar envío {consumption['id']}: {e}")
            conn.rollback()

    # Cerrar cursor y conexión
    cursor.close()
    conn.close()

    return

def upload_returns_inventaria_sheet(returns):

    # Conectar a la base de datos
    conn = psycopg2.connect(
            dbname=INVENTARIA_POSTGRES_DB,
            user=INVENTARIA_POSTGRES_USER,
            password=INVENTARIA_POSTGRES_PASSWORD,
            host=INVENTARIA_POSTGRES_HOST,
            port=INVENTARIA_POSTGRES_PORT
    )
    cursor = conn.cursor()

    # Consulta SQL para insertar o actualizar un envío en la tabla InventariaSheet
    insert_query = sql.SQL("""
        INSERT INTO "inventaria_app_inventariasheet" (
            date, product_id, quantity, 
            cost, movement_type, movement_description, source_id, user_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, product_id, source_id) DO UPDATE SET
            quantity = EXCLUDED.quantity,
            cost = EXCLUDED.cost,
            movement_type = EXCLUDED.movement_type,
            movement_description = EXCLUDED.movement_description,
            user_id = EXCLUDED.user_id;
    """)


    for item in returns:
        # Convertir la fecha de envío a un formato adecuado
        return_date = datetime.strptime(item['return_date'], '%Y-%m-%d')
        return_date = return_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        # Obtener detalles del producto basado en el variant_id
        product = get_product_by_variant_id(item['variant_id'])
        
        if not product:
            print(f"Producto no encontrado para variant_id {item['variant_id']}.")
            continue

        try:
            # Ejecutar la consulta de inserción para cada registro en returns

            cursor.execute(insert_query, (
                return_date,
                product['id'],                      # ID del producto en la tabla relacionada
                item['quantity'],               # Cantidad enviada
                item['variant_cost'],           # Costo de la variante
                'mov_return',                       # Tipo de movimiento
                f"Return ID {item['id']}",    # Descripción del movimiento
                item['id'],                     # ID del movimiento
                int(item['user'])               # ID del usuario responsable del envío
            ))
            conn.commit()
            print(f"Envío registrado: {item['id']} para el producto {product['name']}.")
        except Exception as e:
            print(f"Error al insertar envío {item['id']}: {e}")
            conn.rollback()

    # Cerrar cursor y conexión
    cursor.close()
    conn.close()

    return

def upload_metric_2(metric_2):
    import psycopg2
    from psycopg2 import sql

    # Conectar a la base de datos
    conn = psycopg2.connect(
            dbname=INVENTARIA_POSTGRES_DB,
            user=INVENTARIA_POSTGRES_USER,
            password=INVENTARIA_POSTGRES_PASSWORD,
            host=INVENTARIA_POSTGRES_HOST,
            port=INVENTARIA_POSTGRES_PORT
    )
    cursor = conn.cursor()

    # Consulta SQL para insertar o actualizar en caso de conflicto
    upsert_query = sql.SQL("""
        INSERT INTO "metric2" (
            "date", "createdAt", "updatedAt", 
            "deviation", "net", "companyId"
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT ("date", "companyId") DO UPDATE SET
            "updatedAt" = EXCLUDED."updatedAt",
            "deviation" = EXCLUDED."deviation",
            "net" = EXCLUDED."net";
    """)

    # Convertir la fecha de envío a un formato adecuado
    date = metric_2['date']
    try:
        # Ejecutar la consulta de inserción o actualización
        cursor.execute(upsert_query, (
            date,
            metric_2['createdAt'],  
            metric_2['updatedAt'],              
            metric_2['deviation'],         
            metric_2['net'],                    
            metric_2['companyId']    
        ))
        conn.commit()
        print(f"Métrica 2 procesada correctamente para la fecha {metric_2['date']} y compañía {metric_2['companyId']}.")
    except Exception as e:
        print(f"Error al procesar la métrica 2: {metric_2}: {e}")
        conn.rollback()

    # Cerrar cursor y conexión
    cursor.close()
    conn.close()

    return

def load_existing_products():
    """Carga todos los productos existentes en la base de datos en un diccionario."""
    conn = psycopg2.connect(
            dbname=INVENTARIA_POSTGRES_DB,
            user=INVENTARIA_POSTGRES_USER,
            password=INVENTARIA_POSTGRES_PASSWORD,
            host=INVENTARIA_POSTGRES_HOST,
            port=INVENTARIA_POSTGRES_PORT
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Cargar todos los productos existentes
    cursor.execute("SELECT source_id, warehouse_id, name, description, bar_code, code, category FROM inventaria_app_product;")
    products = cursor.fetchall()

    # Crear un diccionario de productos existentes con (source_id, warehouse_id) como clave
    existing_products = {
        (product['source_id'], product['warehouse_id']): product
        for product in products
    }

    cursor.close()
    conn.close()
    return existing_products

def insert_products_batch(products, existing_products):
    """Inserta un lote de productos, omitiendo aquellos que ya existen en la base de datos."""
    # Filtrar productos que no están en existing_products
    new_products = [
        product for product in products 
        if (product['source_id'], product['warehouse_id']) not in existing_products
    ]

    if not new_products:
        print("No hay productos nuevos para insertar.")
        return

    # Conectar a la base de datos
    conn = psycopg2.connect(
            host=INVENTARIA_POSTGRES_HOST,
            database=INVENTARIA_POSTGRES_DB,
            user=INVENTARIA_POSTGRES_USER,
            password=INVENTARIA_POSTGRES_PASSWORD,
            port=INVENTARIA_POSTGRES_PORT
    )
    cursor = conn.cursor()

    # Inserción masiva
    insert_query = sql.SQL("""
        INSERT INTO "inventaria_app_product" (name, description, warehouse_id, source_id, bar_code, code, category)
        VALUES %s
        ON CONFLICT (source_id, warehouse_id)
        DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, 
                      bar_code = EXCLUDED.bar_code, code = EXCLUDED.code, 
                      category = EXCLUDED.category;
    """)

    values = [
        (
            product['name'],
            product['description'],
            product['warehouse_id'],
            product['source_id'],
            product['bar_code'],
            product['code'],
            product['category']
        )
        for product in new_products
    ]

    try:
        psycopg2.extras.execute_values(
            cursor, insert_query.as_string(cursor), values, template=None, page_size=100
        )
        conn.commit()
        print(f"Batch de {len(new_products)} productos insertado correctamente.")
    except Exception as e:
        conn.rollback()
        print(f"Error al insertar el batch: {e}")
    finally:
        cursor.close()
        conn.close()

    # Actualizar el diccionario existing_products con los nuevos productos
    existing_products.update({
        (product['source_id'], product['warehouse_id']): product
        for product in new_products
    })

def inventaria_upload_variants(variants, product_map, batch_size=50):
    existing_products = load_existing_products()
    for i in range(0, len(variants), batch_size):
        batch = variants[i:i + batch_size]
        new_batch = []

        for variant in batch:
            product_id = variant.get('product_id')
            if product_id is None:
                logger.warning(f"Variant does not have a product_id: {variant}")
                continue
            try:
                product_id = int(product_id)
            except ValueError:
                logger.warning(f"Invalid product_id '{product_id}' for variant {variant}")
                continue

            product = product_map.get(product_id)
            if not product:
                logger.warning(f"Product detail not found for product_id {product_id}")
                continue  # Skip this variant

            if variant['description']:
                variant['name'] = f"{product['name']}-{variant['description']}-{variant['source_id']}"
            else:
                variant['name'] = f"{product['name']}-{variant['source_id']}"


            variant['category'] = product['product_type'] if product['product_type'] else 'Unknown'
            variant['warehouse_id'] = 1
            new_batch.append(variant)

        insert_products_batch(new_batch, existing_products)

    return variants

def get_metrics():

    return

def get_inventaria_metrics(date=None, alert_level=None, alert_type=None):
    """
    Retrieves a list of dictionaries representing metrics (alerts), including associated product data.
    """
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("No se pudo establecer conexión a la base de datos para obtener alertas.")
            return []

        # Determine the query and parameters based on the provided arguments
        if date and alert_level is not None:
            today_str = date.strftime('%Y-%m-%d')
            query = """
                SELECT m.*, p.code AS product_code, p.name AS product_name
                FROM inventaria_app_metric m
                LEFT JOIN inventaria_app_product p ON m.inventaria_product_id = p.id
                WHERE m.date = %s AND m.alert_level = %s;
            """
            params = (today_str, alert_level)
            log_msg = f"Se encontraron {{}} alertas para la fecha {today_str} y nivel de alerta {alert_level}."
        elif alert_type:
            query = """
                SELECT m.*, p.code AS product_code, p.name AS product_name
                FROM inventaria_app_metric m
                LEFT JOIN inventaria_app_product p ON m.inventaria_product_id = p.id
                WHERE m.metric_id = %s;
            """
            params = (alert_type,)
            log_msg = f"Se encontraron {{}} alertas para la métrica {alert_type}."
        else:
            query = """
                SELECT m.*, p.code AS product_code, p.name AS product_name
                FROM inventaria_app_metric m
                LEFT JOIN inventaria_app_product p ON m.inventaria_product_id = p.id;
            """
            params = ()
            log_msg = "Se encontraron {} alertas en total."

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()
            logger.info(log_msg.format(len(results)))

        return results

    except psycopg2.Error as db_err:
        logger.error(f"Error al consultar las alertas: {db_err}")
        return []
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado al obtener las alertas: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_inventaria_sheet_data(start_date, end_date, movement_type):
    """
    Obtiene datos de la tabla InventariaSheet filtrando por tipo de movimiento y rango de fechas.

    Args:
        start_date (str): Fecha de inicio en formato 'YYYY-MM-DD'.
        end_date (str): Fecha de fin en formato 'YYYY-MM-DD'.
        movement_type (str): Tipo de movimiento a filtrar (por defecto 'shipping').

    Returns:
        list: Lista de diccionarios con los datos de InventariaSheet.
    """
    query = """
        SELECT *
        FROM inventaria_app_inventariasheet
        WHERE date BETWEEN %s AND %s
          AND movement_type = %s;
    """

    conn = get_db_connection()
    if not conn:
        logger.error("No se pudo establecer conexión a la base de datos.")
        return []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (start_date, end_date, movement_type))
            records = cursor.fetchall()
            logger.info(f"Se encontraron {len(records)} registros de tipo '{movement_type}' en InventariaSheet.")
            return records
    except psycopg2.Error as db_err:
        logger.error(f"Error al consultar InventariaSheet: {db_err}")
        return []
    finally:
        conn.close()

def get_continuous_alert_days(alerts, product_code, alert_type):
    """
    Cuenta la cantidad de días consecutivos que una alerta específica (stock_critical, zero, low_rotation)
    ha estado activa para un producto específico.

    Args:
        product_name (char): Nombre del producto.
        alert_type (str): Tipo de alerta ('stock_critical', 'zero', 'low_rotation').

    Returns:
        int: Número de días consecutivos que la alerta ha estado activa.
    """
    try:
        product_alerts = [alert for alert in alerts if alert['product'] == product_code]

        if not product_alerts:
            logger.info(f"No se encontraron alertas de tipo '{alert_type}' para el producto ID {product_code}.")
            return 1

        product_alerts.sort(key=lambda x: x['date'], reverse=True)

        consecutive_days = 1
        previous_date = product_alerts[0]['date']
        previous_date = datetime.strptime(previous_date, '%Y-%m-%d').date()

        for alert in product_alerts[0:]:
            current_date = alert['date']
            current_date = datetime.strptime(current_date, '%Y-%m-%d').date()
            if previous_date == current_date:
                consecutive_days += 1
                continue
            if previous_date - current_date == timedelta(days=1):
                consecutive_days += 1
                previous_date = current_date
            else:
                break 

        return consecutive_days

    except Exception as e:
        logger.error(f"Error al calcular días continuos para el producto {product_code} y alerta '{alert_type}': {e}")
        return 0
    

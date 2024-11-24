
from layers.inventaria_layer.python.inventaria_const import *

def shipping_quantity_mean(shipping_movements, product_id):
    """
    Calcula el promedio de la cantidad de envíos diarios de un producto específico en los últimos 30 días.
    
    Args:
        product_id (int): ID del producto.
    
    Returns:
        float: Promedio de la cantidad de envíos diarios en los últimos 30 días.
    """
    # Filtrar los envíos para el product_id especificado
    product_shippings = [shipping for shipping in shipping_movements if shipping['product_id'] == product_id]

    # Si no hay envíos, devolver 0
    if not product_shippings:
        return 0.0

    # Agrupar los envíos por fecha, sumando las cantidades por día
    daily_totals = {}
    for shipping in product_shippings:
        date = shipping['date']
        if date not in daily_totals:
            daily_totals[date] = 0
        daily_totals[date] += shipping['quantity']
    
    # Ordenar por fecha y limitar a los últimos 30 días
    sorted_dates = sorted(daily_totals.keys(), reverse=True)
    last_30_days = sorted_dates[:30]

    # Calcular el promedio diario de cantidad enviada
    total_quantity_last_30_days = sum(daily_totals[date] for date in last_30_days)
    mean_quantity = total_quantity_last_30_days / 30

    return mean_quantity
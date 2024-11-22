import boto3
from inventaria_const import *
from collections import defaultdict
env = 'dev'

def send_alert_email(subject, metrics):
    # Agrupamos las métricas por metric_id
    metrics_by_type = defaultdict(list)
    for metric in metrics:
        metrics_by_type[metric['metric_id']].append(metric)

    # Generamos el contenido HTML
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Inventaria, alertas diarias</title>
    </head>
    <body>
        <p>Hola a todos,</p>
        <p>Se adjuntan sus alertas diarias.</p>
    """

    total_quantity = 0

    for metric_type, alerts in metrics_by_type.items():
        total_type_quantity = len(alerts)
        total_quantity += total_type_quantity

        if metric_type=='stock_zero':
            metric_name = 'stock cero'
            info =  f"""<p> ℹ️ productos con quiebre de stock</p>"""
            actions = f"""<p>💡 Acciones :</strong> Realizar compras de reabastecimiento de tienda o evaluar continuidad de producto.</p>"""
        
        elif metric_type == 'stock_critical':
            metric_name = 'stock crítico'
            info =  f"""<p> ℹ️ productos que en dos días se agota el stock</p>"""
            actions = f"""<p>💡 Acciones :</strong> Realizar compras de reabastecimiento de tienda.</p>"""

        elif metric_type == 'low_rotation':
            metric_name = 'baja rotación'
            info =  f"""<p> ℹ️ productos que en dos días se agota el stock</p>"""
            actions = f"""<p>💡 Acciones :</strong> Generar acción comercial/liquidación</p>"""

        elif metric_type == 'fix_stock':
            metric_name = 'ajuste de stock'
            info =  f"""<p> ℹ️ </p>"""
            actions = f"""<p></p>"""

        elif metric_type == 'hand_on':
            metric_name = 'sobre stock'
            info =  f"""<p> ℹ️ </p>"""
            actions = f"""<p></p>"""

        else:
            metric_name = 'unknown'
            info = f"""<p></p>"""
            actions = f"""<p></p>"""

        
        top_5_alerts = sorted(alerts, key=lambda x: x['alert_days'], reverse=True)[:5]
        
        html_content += f"""
        <p>🚨 Alerta de <strong> {metric_name.upper()}:</strong></p>
        <ul>"""
        html_content +=  info    

        html_content += f"""
                <p>Hay un total de <strong>{total_type_quantity}</strong> SKU en alerta <strong>{metric_type}</strong>.</p>
             """
        html_content += actions

        # Lista de los "top 5" productos con más días en alerta
        for alert in top_5_alerts:
            product_code = alert.get('product_code', 'Sin código')
            product_name = alert.get('product_name', 'Sin nombre')
            alert_days = alert.get('alert_days', 'N/A')
            html_content += f"<li><strong>{product_code}</strong>: {product_name} ({alert_days})</li>"

        html_content += "</ul>"

    html_content += """
        <p>Saludos,</p>
        <p>Equipo Inventaria 📦</p>
    </body>
    </html>
    """

    # Configuración de email
    sender_email = 'inventariasup23@gmail.com'
    aws_region = 'sa-east-1'
    ses = boto3.client('ses', region_name=aws_region)

    cc_emails = inventaria_emails
    to_emails = agunsa_emails

    if env == 'dev':
        cc_emails = developer_emails
        to_emails = developer_emails

    response = ses.send_email(
        Destination={'ToAddresses': to_emails, 'CcAddresses': cc_emails},
        Message={
            'Body': {
                'Html': {'Charset': 'UTF-8', 'Data': html_content},
                'Text': {'Charset': 'UTF-8', 'Data': 'This is the text part of the email.'},
            },
            'Subject': {'Charset': 'UTF-8', 'Data': subject},
        },
        Source=sender_email,
    )

    return response

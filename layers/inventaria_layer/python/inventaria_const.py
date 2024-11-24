import boto3
import json
import os

# inventaria_const.py
def get_db_config():
    secrets_rds = get_secret("prod/inventaria/rds/inventaria_db")
    return {
        "host": secrets_rds.get("host", "localhost"),
        "db": secrets_rds.get("dbInstanceIdentifier", "default_db"),
        "user": secrets_rds.get("username", "default_user"),
        "password": secrets_rds.get("password", "default_password"),
        "port": "5432",
    }

def get_secret(secret_name, region_name="us-east-1"):
    # Crear un cliente de Secrets Manager
    client = boto3.client("secretsmanager", region_name=region_name)
    
    try:
        # Obtener el secreto
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        return secret
    except Exception as e:
        print(f"Error al obtener el secreto: {e}")
        return None

secrets_aws = get_secret("prod/inventaria/aws_cli")
secrets_rds = get_secret("prod/inventaria/rds/inventaria_db")

# Asignar variables desde Secrets Manager o con valores predeterminados
INVENTARIA_POSTGRES_DB = secrets_rds.get("dbInstanceIdentifier", "default_db")
INVENTARIA_POSTGRES_USER = secrets_rds.get("username", "default_user")
INVENTARIA_POSTGRES_PASSWORD = secrets_rds.get("password", "default_password")
INVENTARIA_POSTGRES_HOST = secrets_rds.get("host", "localhost")
AWS_SECRET_ACCESS_KEY = secrets_aws.get("AWS_SECRET_ACCESS_KEY", "")
AWS_ACCESS_KEY_ID = secrets_aws.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = secrets_aws.get("AWS_SECRET_ACCESS_KEY", "")

print(f"INVENTARIA_POSTGRES_HOST : {INVENTARIA_POSTGRES_HOST}")
print(f"[DEBUG] INVENTARIA_POSTGRES_HOST: {INVENTARIA_POSTGRES_HOST}")

# BUCKET_NAME = secrets.get("BUCKET_NAME", "default_bucket")
# WHATSAPP_ACCESS_TOKEN = secrets.get("WHATSAPP_ACCESS_TOKEN", "")

RED_ALERT = 10
WARNING_ALERT = 5
GREEN_ALERT = 0

# Datos de conexión a la base de datos
INVENTARIA_POSTGRES_DB="inventaria_db"
INVENTARIA_POSTGRES_USER="inventaria" 
INVENTARIA_POSTGRES_PASSWORD="NhQsFpmSjD3LwQc"
INVENTARIA_POSTGRES_HOST="inventaria-db.ck37szplgscc.sa-east-1.rds.amazonaws.com"
INVENTARIA_POSTGRES_PORT="5432"

# Datos de BSALE - FARMACIA 
BSALE_BASE_URL = "https://api.bsale.io/v1"
BSALE_ACCESS_TOKEN = "10fe11c752e82a0159f61cbf40791b96b287fbf9"

BUCKET_NAME = 'inventaria-blanket-bucket'
BUCKET_PATH = f'https://7vu8t59ns8.execute-api.sa-east-1.amazonaws.com/dev/{BUCKET_NAME}/'    #inventaria
TOKEN_PATH = '/private/token.json'
SCOPES_DIC = {
    'drive' : ['https://www.googleapis.com/auth/drive.readonly'],
    'gmail' : ['https://www.googleapis.com/auth/gmail.readonly'],
    'sheet' : ['https://www.googleapis.com/auth/spreadsheets'],
    }

SCOPES = [scope[0] for scope in SCOPES_DIC.values()]

#database
FIX_RESULTS_FILE = 'results_fix.json'
TMP_PATH = '\tmp'

#whatsapp
WHATSAPP_ACCESS_TOKEN = "EAAFOxRUXqHYBO6W2dPszg0TcpqvLVjkWg24E3gpCEnZCYalkCcFGXYNCgqL4Ta9Q8ITmAzsI6uQfcZCRsOeOHvjiylJB2AfJFgB06H3gT2HV9IViTzO8LADi2j5gCIZCYpA0OyoAxbHfbU3i3BG76gMspZASzN8zXFLZA0H6RRX7lt09VE2b4oCuZAU4mcH8MsyDHKB06w3MDQEfYTBNiZAYU0Q7BAYWLKZAjuAGCyNxK65B"
WHATSAPP_RECIPIENT_WAID="56945515501"
WHATSAPP_PHONE_NUMBER_ID = "221087547753088"
WHATSAPP_VERSION = "v18.0"

WHATSAPP_APP_ID = "368083346106486"
WHATSAPP_APP_SECRET = "49a6d3e09b3b9fdda1d5532dde1b1f57"

#agunsa
DATE_FILE_RECIVED_COL='Fecha Recibida'
AGUNSA_LOTE = 'Lote'
AGUNSA_CANT_UBIC = 'Cantidad Ubicada'
AGUNSA_NOMBRE = 'Nombre'
AGUNSA_UBIC = 'Ubicación'
AGUNSA_PRODUCTO = 'Producto'

AGUNSA_CONTROL_1='Sobrante inventario SIL'                  #9999
AGUNSA_CONTROL_2='Diferencia de picking'                    #4600
AGUNSA_CONTROL_3='Faltante inventario SIL'                  #4700
AGUNSA_CONTROL_4='Diferencia inv. cíclico'                  #MESAORDENA
AGUNSA_CONTROL_5='Diferencia cíclicos'                      #DIFCICLICO
AGUNSA_CONTROL_6='Diferencia operacional'                   #DIFOPERACI
AGUNSA_CONTROL_7='Permanencia losa recepción'               #RECSCL1

AGUNSA_CONTROL_8='Control Mb51'                             #mb51
AGUNSA_CONTROL_9='Stock SAP'                                #mb52

AGUNSA_CONTROL_10='Comparacion'                             #mb52

agunsa_emails = ["juan.ibarra@report.cl",
"RODOLFO.CASTILLO@report.cl",
"anita.valdebenito@report.cl",
"roberto.castro@report.cl",
"yoselin.cisternas@report.cl",
"daniela.cantillano@report.cl",
"alexis.yanez@report.cl",
"mauricio.montenegro@report.cl"]

alert_emails = ["RODOLFO.CASTILLO@report.cl"                
                ]

inventaria_emails = ["bemarchant@proton.me", 
"jesuspirquilaf@gmail.com", 
"inventariasup23@gmail.com"]

developer_emails = ["bemarchant@proton.me"]

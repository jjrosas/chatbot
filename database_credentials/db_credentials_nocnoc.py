# db_credentials_nocnoc.py
from dotenv import load_dotenv
import os

# Carga las variables de entorno desde el archivo .env
load_dotenv()

# Define el diccionario de credenciales utilizando las variables de entorno
db_credentials_nocnoc = {
    'postgres_admin': {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '5432'),   # Usa 5432 como valor predeterminado si no está en el .env
        'dbname': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
}

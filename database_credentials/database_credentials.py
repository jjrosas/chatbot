from dotenv import load_dotenv
import os

load_dotenv()  # Carga las variables de entorno desde .env

db_credentials_nocnoc = {
    'postgres_admin': {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '5432'),
        'dbname': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
}
from sqlalchemy import create_engine
from database_credentials.db_credentials_nocnoc import db_credentials_nocnoc as creds

def get_engine(creds):
    return create_engine(
        f"postgresql://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['dbname']}"
    )

# Usa el engine
try:
    engine = get_engine(creds['postgres_admin'])
    print("Conexión creada exitosamente:", engine)
except Exception as e:
    print(f"Error al crear conexión: {e}")

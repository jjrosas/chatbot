import pandas as pd
from sqlalchemy import create_engine
def get_data(query, connection_credentials):
    """
    Ejecuta una consulta SQL y devuelve un DataFrame con los resultados.

    Args:
        query (str): Consulta SQL a ejecutar.
        connection_credentials (dict): Credenciales de conexión a la base de datos.

    Returns:
        pd.DataFrame: Resultados de la consulta.
    """
    import psycopg2
    import pandas as pd

    # Construir la cadena de conexión desde el diccionario
    connection_string = (
        f"host={connection_credentials['host']} "
        f"port={connection_credentials['port']} "
        f"dbname={connection_credentials['dbname']} "
        f"user={connection_credentials['user']} "
        f"password={connection_credentials['password']}"
    )

    # Conectarse a la base de datos y ejecutar la consulta
    try:
        with psycopg2.connect(connection_string) as conn:
            return pd.read_sql(query, conn)
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
        raise
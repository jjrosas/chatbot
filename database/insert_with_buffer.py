import io
from typing import Union
import pandas as pd
import psycopg2
from .split_conn_string import split_conn_string


def insert_with_buffer(df: pd.DataFrame,
                    conn_auth:Union[str,dict],
                    table:str,
                    schema:str,
                    header:bool=False):
    
    """
    Insert a pandas DataFrame into a PostgreSQL table using a StringIO buffer and the COPY command.
    
    Parameters:
        df (pandas.DataFrame): The dataframe to be inserted.
        conn_auth (Union[str, dict]): The connection string or dictionary containing the connection information.
        table (str): The name of the table to insert data into.
        schema (str): The name of the schema in which the table is located.
        header (bool, optional): Indicates whether the first row of the dataframe should be treated as header. Defaults to False.
    
    Returns:
        None
    
    Raises:
        Exception: Raises an error if an exception occurs during the insert process.
        psycopg2.DatabaseError: Raises an error if a database error occurs during the insert process.
    
    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        >>> conn_auth = "postgresql://user:password@host:port/db"
        >>> table = "table_name"
        >>> schema = "schema_name"
        >>> header = False
        >>> insert_with_buffer(df, conn_auth, table, schema, header)
    """

    if isinstance(conn_auth,str):
        conn_auth = split_conn_string(conn_auth)
    
    for key in conn_auth.keys():
        if key not in ['user','password','host','port','db']:
            conn_auth.pop(key,None)

    connection = psycopg2.connect(
            **conn_auth
        )
    connection.autocommit = True
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=header)
    buffer.seek(0)

    with connection.cursor() as cursor:        
        try:
            cursor.copy_expert(f"COPY postgres.{schema}.{table} FROM STDIN (FORMAT 'csv', HEADER {str(header).lower()})" , buffer)
        except Exception  as error:
            print("Error: %s" % error)
            if "data" in str(error).lower() or 'style' in str(error).lower() :
                print("Remember: The dataframe must contain all the columns in the extact same order and all columns should be present. In case a default column exist in a table then should exists in the dataframe as well")
                
            raise error
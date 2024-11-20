import sys
import pandas as pd
import io
import json
import numpy as np
import psycopg2
import boto3
import base64
from typing import Union
from datetime import datetime
from sqlalchemy import create_engine,text
import os
import logging


def get_data(query, connection_credentials):

    """
    Return a df from query made to db in conection credentials
    query: str
        string stating db query in mysql or postgres format
    conection_credentials: str
        string with credentials to connect to db
    """
    if 'redshift' in connection_credentials.lower():
        engine = create_engine(connection_credentials, connect_args={'sslmode': 'prefer'})
    else:
        engine = create_engine(connection_credentials)
    conn = engine.connect()

    try:
        if 'show' not in query.lower():
            df = pd.read_sql_query(query,conn)
        else:
            df = conn.execute(query).fetchall()

        conn.invalidate()
        engine.dispose()
    except Exception as e:
        raise e

    finally:
        conn.invalidate()
        engine.dispose()

    return df


def execute_sql(sql_statement:str,creds:str):

    """
    Execute a SQL statement against a database using SQLAlchemy.
    
    Parameters:
    - sql_statement (str): The SQL statement to be executed.
    - creds (str): Connection string for the database in the format 'dialect+driver://username:password@host:port/database'.
    
    Returns:
    - int: 1 if the statement was executed successfully, 0 otherwise.
    """

    db = create_engine(creds)

    with db.connect() as con:
        con.execute(text(sql_statement))

    db.dispose()

    return 1


def split_conn_string(conn_string: str) -> dict:

    """
    Split a connection string into individual components for use in connecting to the database.
    
    Parameters:
        conn_string (str): The connection string.
    
    Returns:
        tuple: A dictionary containing the following keys: user, password, host, port, and db.
    
    Example:
        >>> conn_string = "postgresql://user:password@host:port/db"
        >>> split_conn_string(conn_string)
        {'user': 'user', 'password': 'password', 'host': 'host', 'port': 'port', 'db': 'db'}
    """

    
    elements = conn_string.split("://")
    conn_type, rest = elements[0], elements[1]

    if "mysql" in conn_type.lower():
        conn_type = "mysql"
    elif "postgresql" in conn_type.lower():
        conn_type = "postgresql"

    user_pass, host_port_db = rest.split("@")
    user, password = user_pass.split(":")
    host_port, db = host_port_db.split("/") if "/" in host_port_db else (host_port_db, None)
    host, port = host_port.split(":")

    return {
        "user":user,
        "password":password,
        "host":host,
        "port":port,
        "db":db
    }


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
    

def retrieve_secret(
    secret: str,
    region: str = 'us-east-1',
):
    """Retrieve json data for secret in AWS Secret Manager.

    Parameters
    ----------
    secret : str
        secret name
    region : str
        aws region name
    """

    # Create a Secrets Manager client
    try:
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region
        )

        # try:
        get_secret_value_response = client.get_secret_value(SecretId=secret)

        if 'SecretString' in get_secret_value_response:
            return json.loads(get_secret_value_response['SecretString'])
        else:
            return base64.b64decode(get_secret_value_response['SecretBinary'])
    except Exception as e:
        try:
            return json.loads(os.environ.get(secret))
        except:
            raise e


def _build_conn_string(conn_type:str,user:str,password:str,host:str,port,db=None)->str:

    if 'mysql' in conn_type.lower() or 'mariadb' in conn_type.lower():
        conn_type="mysql+mysqlconnector"
    elif 'postgres' in conn_type.lower():
        conn_type = "postgresql+psycopg2"

    conn_string = f"{conn_type}://{user}:{password}@{host}:{port}"

    if db is not None:
        conn_string = conn_string+'/'+db

    return conn_string


def build_connection_string(secret_name:str):

    connection_data = retrieve_secret(secret_name)

    if isinstance(connection_data,str):
        connection_data = json.loads(connection_data)

    conn_type_raw = 'mariadb' if 'nocnoc' in connection_data.get('dbname') else 'postgres'
    conn_type = conn_type_raw
    host = connection_data['host']
    db = connection_data['dbname']
    port = connection_data['port']
    user = connection_data['username']
    password = connection_data['password']

    conn_string = _build_conn_string(
        conn_type=conn_type,
        user=user,
        password=password,
        host=host,
        port=port
    )

    return conn_string

query_seller_product_merchants = """
SELECT
    id,
    CASE when cast(substr(DATE_FORMAT(created_at, '%%Y'),1,4) as integer) > 1900 then created_at else null end as created_at,
    CASE when cast(substr(DATE_FORMAT(updated_at, '%%Y'),1,4) as integer) > 1900 then updated_at else null end as updated_at,
    seller_product_id,
	merchant_id,
	country_iso,
	amount_usd,
	other_fee_usd,
	shipping_usd,
	tax_usd,
	payout_amount,
	commission_usd,
	payin_amount,
	risk_amount,
	discount_usd,
	marketplace_fee_amount,
	total_amount_usd,
	fx_rate,
	total_local_amount,
	diff,
	total_published_amount,
	published_currency,
	locked,
	online,
	enabled,
	actual_change_id,
	status,
	updated
FROM {db_schema}.seller_product_merchants WHERE updated_at > '{incremental_bookmark}' 
order by updated_at
"""


query_seller_product_merchant_changes = """
SELECT
    id,
	CASE when cast(substr(DATE_FORMAT(created_at, '%%Y'),1,4) as integer) > 1900 then created_at else null end as created_at,
    CASE when cast(substr(DATE_FORMAT(updated_at, '%%Y'),1,4) as integer) > 1900 then updated_at else null end as updated_at,
	seller_product_merchant_id,
	seller_product_id,
	merchant_id,
	country_iso,
	amount_usd,
	other_fee_usd,
	shipping_usd,
	tax_usd,
	payout_amount,
	commission_usd,
	payin_amount,
	risk_amount,
	discount_usd,
	marketplace_fee_amount,
	total_amount_usd,
	fx_rate,
	total_local_amount,
	diff,
	total_published_amount,
	published_currency,
	locked,
	online,
	enabled,
	seller_product_change_id,
	status
FROM {db_schema}.seller_product_merchant_changes WHERE updated_at > '{incremental_bookmark}'
order by updated_at
"""


insert_landing_bookmark = """
    insert into airflow.landing_bookmarks  (data_type,
                                    min_data_timestamp,
                                    max_data_timestamp,
                                    inserted_at,
                                    table_name)
    values('{etl_name}',
            '{min_updated_at}',
            '{max_updated_at}',
            now(),
            '{table_name}'
            );
        """

if __name__=='__main__':
    
    # parse arguments
    for i,value in enumerate(sys.argv):
        if 'table' in value:
            table_name = sys.argv[i+1]

    incremental_key = 'updated_at'

    print(f'starting load for {table_name}')
    db_schema = 'nocnoc' if table_name=='seller_product_merchants' else 'nocnoc_log'
    query = query_seller_product_merchants if table_name == 'seller_product_merchants' else query_seller_product_merchant_changes
    
    etl_name = f"etl_mariadb_to_postgres_{table_name}"
    
    print('Building connection string for source')
    conn_string_source = build_connection_string('mariadb_connection').replace('mysqlconnector','pymysql')

    print('Building connection string for target')
    conn_string_target = build_connection_string('postgres_connection')

    print('Getting last updated at')
    incremental_bookmark = f"SELECT MAX(updated_at) last_updated_at from landing.{table_name}"

    incremental_bookmark = get_data(incremental_bookmark,conn_string_target)['last_updated_at'].iloc[0]

    print(f'Last updated {incremental_bookmark}')
    query = query.format(db_schema=db_schema,incremental_bookmark=incremental_bookmark)

    print(f'*****************Query************:\n{query}')

    # get data from source
    df = get_data(query,conn_string_source)

    df['synced_at'] = pd.Timestamp.now()

    df = df[get_data(f'select * from landing.{table_name} limit 0',conn_string_target).columns]

    if 'seller_product_change_id' in df.columns:
        df['seller_product_change_id'] = df['seller_product_change_id'].astype('Int64')

    n_parts = max(df.shape[0]//100_000,1)

    print(f"Dataframe of {df.shape[0]} will be splitted in {n_parts} parts")
    min_updated_at = df.updated_at.min()
    max_updated_at = df.updated_at.max()
    
    print(f"Uploading info to postgres")
    for df_i in np.array_split(df,n_parts):
        print(datetime.now(),'starting the update',i)
        insert_with_buffer(df_i,
                        conn_string_target,
                        table_name,
                        schema='landing',
                        header=True)

    print(f"Inserting bookmark")
    insert_landing_bookmark = insert_landing_bookmark.format(
                    etl_name=etl_name,
                    min_updated_at=min_updated_at,
                    max_updated_at=max_updated_at,
                    table_name=table_name
                )

    execute_sql(insert_landing_bookmark,conn_string_target)
        
    print(f"Process done!")
    
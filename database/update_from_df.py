from .upload_table import upload_table
from .execute_sql import execute_sql
from datetime import datetime

def upload_from_df(
    df,
    conn_string:str,
    target_table:str,
    target_schema:str,
    match_columns:list,
    update_columns:list
    ):

    temp_table_name = 'temp_update_'+datetime.now().strftime('%Y%m%d%M%S')

    upload_table(
        df=df,
        conn_string=conn_string,
        table_name=temp_table_name,
        schema='development',
        if_exists='replace'
    )

    update_columns_clause = ','.join([f"{x} = temp_t.{x}" for x in update_columns])
    where_clause = ' AND '.join([f"t.{x} = temp_t.{x}" for x in match_columns])

    update_statement = f"""
                update {target_schema}.{target_table} t
                set {update_columns_clause}
                from development.{temp_table_name} temp_t
                where {where_clause}
                """

    execute_sql(update_statement,conn_string)

    execute_sql(f"drop table IF EXISTS development.{temp_table_name}",conn_string)


def update_from_df(
    df,
    conn_string:str,
    target_table:str,
    target_schema:str,
    match_columns:list,
    update_columns:list
    ):

    temp_table_name = 'temp_update_'+datetime.now().strftime('%Y%m%d%M%S')

    upload_table(
        df=df,
        conn_string=conn_string,
        table_name=temp_table_name,
        schema='development',
        if_exists='replace'
    )

    update_columns_clause = ','.join([f"{x} = temp_t.{x}" for x in update_columns])
    where_clause = ' AND '.join([f"t.{x} = temp_t.{x}" for x in match_columns])

    update_statement = f"""
                update {target_schema}.{target_table} t
                set {update_columns_clause}
                from development.{temp_table_name} temp_t
                where {where_clause}
                """

    execute_sql(update_statement,conn_string)

    execute_sql(f"drop table IF EXISTS development.{temp_table_name}",conn_string)


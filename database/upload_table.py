from sqlalchemy import create_engine
import pandas as pd

def upload_table(df:pd.DataFrame,
                conn_string:str,
                table_name:str,
                if_exists='replace',
                index=False,
                index_label=None,
                db_type='postgres',
                schema=None,
                chunksize=1024):
    
    if db_type =='mysql':
        if index == False and index_label==None:
            df = df.reset_index()
            df.rename(columns={'index':'id'},inplace=True)

    engine = create_engine(conn_string)
    conn = engine.connect()

    if db_type== 'mysql':
        # Handling sql_require_primary_key parameter of Digital Ocean
        show_command = "show variables like 'sql_require_primary_key';"
        sql_require_primary_key = conn.execute(show_command).fetchall()[0][1]
        if sql_require_primary_key==1 or sql_require_primary_key=='ON':
            conn.execute("SET SESSION sql_require_primary_key = 0;")
            sql_require_primary_key = conn.execute(show_command).fetchall()[0][1]

    try:
        df.to_sql(
            name = table_name,
            if_exists=if_exists,
            con= conn,
            chunksize=chunksize,
            method='multi',
            index=index ,
            index_label=index_label,
            schema=schema
        )

        if db_type=='mysql':
            conn.execute(f'ALTER TABLE {table_name} ADD PRIMARY KEY (id);')

    except Exception as e:
        raise(e)

    finally:
        conn.invalidate()
        engine.dispose()
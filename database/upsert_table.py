# https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#insert-on-conflict-upsert


from pandas import notnull, DataFrame
from sqlalchemy import create_engine,text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, MetaData
from typing import Union
from numpy import nan


def upsert_redshift(df:DataFrame,
                    conn_string:str,
                    table:str,
                    schema:str,
                    pk:list,
                    on_conflict:str='do_nothing'
                    ):

    # create temp table

    db = create_engine(conn_string)

    with db.connect() as con:

        con.execute(text(f"create temp table temp_{table}(like {schema}.{table});").execution_options(autocommit=True))
        soure_table_full_nm = f"temp_{table}"
        target_table_full_nm = f"{schema}.{table}"
        source_table_alias  = f"temp_{table}"
        target_table_alias = table

        list_col_update = pk if on_conflict=='do_nothing' else df.columns
        update_stmt = "UPDATE SET {cols}".format(cols = ",".join([f"{x} = {source_table_alias}.{x}" for x in list_col_update]))
        match_codition = ' and '.join([f"{target_table_alias}.{x} = {source_table_alias}.{x}" for x in pk])
        columns = ','.join(df.columns)
        insert_stmt =  ','.join([f"{source_table_alias}.{x}" for x in df.columns])
        merge_stmt = f"""
            MERGE INTO {target_table_full_nm} USING {soure_table_full_nm}
            ON {match_codition}
            WHEN MATCHED THEN {update_stmt}
            WHEN NOT MATCHED THEN INSERT ({columns}) values ({insert_stmt});
        """

        df.to_sql(
                name = soure_table_full_nm,
                if_exists='append',
                con= con,
                chunksize=1024,
                method='multi',
                index=False,
                index_label=None,
            )

        con.execute(text(merge_stmt).execution_options(autocommit=True))

    db.dispose()

def upsert_dataframe(df:DataFrame,
                    conn_string:str,
                    table:str,
                    schema:str,
                    pk:Union[str,list],
                    on_conflict:str='do_nothing',
                    chunksize=100,
                    tqdm=False):

    """
    Inserts or updates rows in a database table using a DataFrame and SQLAlchemy.

    Parameters:
    - df (pd.DataFrame): The DataFrame containing the data to be inserted or updated.
    - conn_string (str): Connection string for the database in the format 'dialect+driver://username:password@host:port/database'.
    - table (str): The name of the table to insert or update the data.
    - schema (str): The schema of the table
    - pk (Union[str,list]): The primary key or list of primary keys of the table.
    - on_conflict (str): The conflict resolution strategy. 'do_nothing' for doing nothing and 'update' for updating the existing rows.
    - chunksize (int): The number of rows to be inserted or updated at a time.
    - tqdm (bool): Whether to display a progress bar or not.

    Returns:
    - None
    """
    df = df.where(notnull(df), None)
    df = df.replace({nan: None})


    if isinstance(pk,str):
        pk = [pk]

    if 'redshift' in conn_string:

        upsert_redshift(
                    df=df,
                    conn_string=conn_string,
                    table=table,
                    schema=schema,
                    pk=pk,
                    on_conflict=on_conflict
        )
        return None

    engine = create_engine(conn_string)

    metadata = MetaData()
    # auto-map db tables, list down the tables you want to query here
    metadata.reflect(engine, schema=schema,only=[table])

    Base = automap_base(metadata=metadata)

    Base.prepare()

    table_map = Base.classes.get(table)
    try:
        stmt = insert(table_map).values(df.to_dict(orient='records'))
    except Exception as e:
        if 'not-null constraint' in str(e) or 'not-null constraint' in str(e.__traceback__):
            print('Remember to define a constraint in the target table (pk or unique)')
        raise e

    if on_conflict=='do_nothing':
        stmt  = stmt.on_conflict_do_nothing(index_elements = pk)

    if on_conflict=='update':
        stmt = stmt.on_conflict_do_update(index_elements = pk,
                                        set_ = dict(stmt.excluded.items()))

    with engine.connect() as conn:
        conn.execute(stmt.execution_options(autocommit=True))
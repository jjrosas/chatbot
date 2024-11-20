from sqlalchemy import create_engine,text

def execute_sql(sql_statement:str,creds:str):

    db = create_engine(creds)

    with db.connect() as con:
        con.execute(text(sql_statement).execution_options(autocommit=True))
        # con.commit()

    db.dispose()

    return 1
import logging
from sqlalchemy import create_engine
from datetime import datetime

def grant_permissions(schema:str, postgres_cred:str,role_list:list):
    
    logging.info('Granting access')

    roles = ','.join(role_list)

    access_statement = f"""GRANT USAGE ON SCHEMA {schema} TO {roles};
                          GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {roles};
                          GRANT SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO {roles};"""
    
    db = create_engine(postgres_cred)
    db.execute(access_statement)
    db.dispose()
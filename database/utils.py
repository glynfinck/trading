import pandas as pd
import psycopg2
from prefect import flow, task

from sqlalchemy import create_engine 
from sqlalchemy.dialects.postgresql import insert
from prefect.variables import Variable

def postgres_upsert(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    result = conn.execute(upsert_statement)
    return result

@task
def query_table(query: str):
    digital_ocean_credentials: dict = Variable.get("digital_ocean_credentials")
    with psycopg2.connect(digital_ocean_credentials.get("connection_string")) as conn:
        return pd.read_sql_query(query, conn)

@task
def set_data(table_name: str, data: pd.DataFrame, context=None):
    digital_ocean_credentials: dict = Variable.get("digital_ocean_credentials")
    engine = create_engine(digital_ocean_credentials.get("connection_string"))
    message = f"Setting {len(data)} row(s) to {table_name}"
    if context != None:
        context.log.info(message)
    else:
        print(message)
    data.to_sql(table_name, engine, if_exists='append',index=False, method=postgres_upsert)
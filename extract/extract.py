from sqlalchemy import create_engine
import pandas as pd
import traceback
from util.db_postgres import DB_Postgres as db

def extract(table:str):
    try:
        dbPostgres = db("oltp")
        cursor = dbPostgres.start()  
        
        if cursor is None:
            raise Exception("No se pudo obtener un cursor válido")

        engine = create_engine(dbPostgres.connection_string() ) # Usa la cadena de conexión de dbPostgres

        query = f"SELECT * From {table}"
        roles_df = pd.read_sql(query, engine) 
        print(roles_df)

        return roles_df

    except Exception as e:
        print("Ocurrió un error:", str(e))
        traceback.print_exc()

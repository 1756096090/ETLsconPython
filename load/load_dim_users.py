from sqlalchemy import create_engine
import pandas as pd
import traceback
from util.db_postgres import DB_Postgres as db

def load_users():
    try:
        print("Iniciar Carga de users al sor")
        
        dbPostgres = db("staging")
        cursor = dbPostgres.start()  

        if cursor is None:
            raise Exception("No se pudo obtener un cursor válido")

        engine = create_engine(dbPostgres.connection_string()) 
        
        dbPostgresSor = db("sor")
        cursorSor = dbPostgresSor.start()
        
        if cursorSor is None:
            raise Exception("No se pudo obtener un cursor válido para el Sor")
        
        engineSor = create_engine(dbPostgresSor.connection_string())  

        query = "SELECT * FROM ext_users" 
        users_df = pd.read_sql(query, engine)

        users_df = users_df.rename(columns={'id': 'id_users_bk'})
        users_df = users_df.drop(columns=['activated_at', 'created_at', 'email_verified_at', 'id_role'])

        print("Datos transformados:")
        print(users_df)

        users_df.to_sql("dim_users", engineSor, if_exists='append', index=False)

        print("Finalizar carga de users al Sor")

    except Exception as e:
        print("Ocurrió un error:", str(e))
        traceback.print_exc()

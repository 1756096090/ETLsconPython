from sqlalchemy import create_engine
import pandas as pd
import traceback
from util.db_postgres import DB_Postgres as db

def load_companies():
    try:
        print("Iniciar Carga al sor")
        
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

        query = "SELECT * FROM tra_companies"
        companies_df = pd.read_sql(query, engine)

        companies_df = companies_df.rename(columns={'company_id': 'id_company_bk'})

        print(companies_df)

        companies_df.to_sql("dim_companies", engineSor, if_exists='append', index=False)

        print("Finalizar carga al Sor")

    except Exception as e:
        print("Ocurrió un error:", str(e))
        traceback.print_exc()

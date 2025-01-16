from sqlalchemy import create_engine
import traceback
from util.db_postgres import DB_Postgres as db

def persistir_staging(df_stag, tab_name, if_exist = 'replace' ):
    try:
        
        print ("Persistir Staging")
        dbPostgres = db("staging")
        dbPostgres.start() 
        engine = create_engine(dbPostgres.connection_string())  
        df_stag.to_sql(tab_name, engine, if_exists=if_exist, index=False)
        print("Finalizado Persistir Staging")
    
    except Exception as e:
        print("Ocurrió un error:", str(e))
        traceback.print_exc()

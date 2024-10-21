import traceback
import pandas as pd
from util.db_connection import Db_Connection

def persistir_staging(df_stag, tab_name):
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'root'
        db = 'staging'
        
        con_db = Db_Connection(type, host, port, user, pwd, db)
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        df_stag.to_sql(tab_name, ses_db, if_exists='replace', index= False)
            
    except:
        traceback.print_exc()
    finally:
        pass
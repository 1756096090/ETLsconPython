import traceback
import pandas as pd
from util.db_connection2 import Db_Connection

def extraer_film():
    try:
        con_db = Db_Connection('oltp')
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        address = pd.read_sql ('SELECT *FROM film; ', ses_db)
        return address
            
    except:
        traceback.print_exc()
    finally:
        pass